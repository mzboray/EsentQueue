using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Isam.Esent;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Vista;
using MsgPack.Serialization;

namespace EsentQueue
{
    public class PersistentQueue<T> : IDisposable
    {
        private readonly static MessagePackSerializer<T> _serializer = MessagePackSerializer.Get<T>();

        private readonly QueueCursorCache _cursorCache;
        private readonly string _databaseName;
        private readonly Instance _instance;

        public PersistentQueue(string path, StartOption startOption)
        {
            _instance = new Instance("EsentQueue.PersistentQueue");
            _instance.Parameters.CreatePathIfNotExist = true;
            _instance.Parameters.CircularLog = true;
            _instance.Parameters.MaxVerPages = 256;
            _instance.Init();
            _databaseName = path;
            _cursorCache = new QueueCursorCache(_instance, _databaseName);

            if (!File.Exists(_databaseName) || startOption == StartOption.CreateNew)
            {
                CreateDatabase();
            }
            else
            {
                CheckDatabase(startOption);
            }
        }

        public int Count
        {
            get
            {
                var cursor = _cursorCache.GetCursor();
                try
                {
                    int count = 0;
                    Api.MoveBeforeFirst(cursor.Session, cursor.DataTable);
                    while (Api.TryMoveNext(cursor.Session, cursor.DataTable))
                    {
                        count++;
                    }

                    return count;
                }
                finally
                {
                    _cursorCache.Return(cursor);
                }
            }
        }

        public void Enqueue(T item)
        {
            var cursor = _cursorCache.GetCursor();
            try
            {
                using (var transaction = cursor.BeginTransaction())
                {
                    using (var update = cursor.CreateDataTableUpdate())
                    {
                        using (var colStream = new ColumnStream(cursor.Session, cursor.DataTable, cursor.SerializedObjectColumn))
                        {
                            _serializer.Pack(colStream, item);
                        }

                        update.Save();
                    }

                    transaction.Commit(CommitTransactionGrbit.LazyFlush);
                }
            }
            finally
            {
                _cursorCache.Return(cursor);
            }
        }

        public QueueItem<T> Dequeue()
        {
            QueueItem<T> item;
            if (!TryDequeue(out item))
            {
                throw new InvalidOperationException("No items in the queue.");
            }

            return item;
        }

        public bool TryDequeue(out QueueItem<T> item)
        {
            var cursor = _cursorCache.GetCursor();
            try
            {
                using (var transaction = cursor.BeginTransaction())
                {
                    Api.JetSetCurrentIndex(cursor.Session, cursor.DataTable, null);
                    Api.MakeKey(cursor.Session, cursor.DataTable, 0L, MakeKeyGrbit.NewKey);

                    if (Api.TrySeek(cursor.Session, cursor.DataTable, SeekGrbit.SeekGE))
                    {
                        int attempts = 1 + (Api.RetrieveColumnAsInt32(cursor.Session, cursor.DataTable, cursor.AttemptCountColumn) ?? 0);
                        using (var colStream = new ColumnStream(cursor.Session, cursor.DataTable, cursor.SerializedObjectColumn))
                        {
                            T obj = _serializer.Unpack(colStream);
                            item = new QueueItem<T>(obj, attempts);
                        }

                        Api.JetDelete(cursor.Session, cursor.DataTable);
                        transaction.Commit(CommitTransactionGrbit.LazyFlush);

                        return true;
                    }
                }

                item = default(QueueItem<T>);
                return false;
            }
            finally
            {
                _cursorCache.Return(cursor);
            }
        }

        public QueueItem<T> Peek()
        {
            QueueItem<T> item;
            if (!TryPeek(out item))
            {
                throw new InvalidOperationException("No items in the queue.");
            }

            return item;
        }

        public bool TryPeek(out QueueItem<T> item)
        {
            var cursor = _cursorCache.GetCursor();
            try
            {
                using (var transaction = cursor.BeginTransaction())
                {
                    if (Api.TryMoveFirst(cursor.Session, cursor.DataTable))
                    {
                        int attempts = 1 + Api.EscrowUpdate(cursor.Session, cursor.DataTable, cursor.AttemptCountColumn, 1);
                        using (var colStream = new ColumnStream(cursor.Session, cursor.DataTable, cursor.SerializedObjectColumn))
                        {
                            T obj = _serializer.Unpack(colStream);
                            item = new QueueItem<T>(obj, attempts);
                        }

                        return true;
                    }
                }

                item = default(QueueItem<T>);
                return false;
            }
            finally
            {
                _cursorCache.Return(cursor);
            }
        }

        public void Dispose()
        {
            _cursorCache?.FreeAll();
            _instance?.Dispose();
        }

        private void CheckDatabase(StartOption startOption)
        {
            // For now assume everything is correct
        }

        private void CreateDatabase()
        {
            using (var session = new Session(_instance))
            {
                JET_DBID dbid;

                Api.JetCreateDatabase(session, _databaseName, null, out dbid, CreateDatabaseGrbit.OverwriteExisting);

                JET_TABLEID tableid;
                JET_COLUMNID colid;
                JET_COLUMNDEF colDef;

                using (var transaction = new Transaction(session))
                {
                    Api.JetCreateTable(session, dbid, "Data", 16, 100, out tableid);

                    colDef = new JET_COLUMNDEF()
                    {
                        coltyp = VistaColtyp.LongLong,
                        grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnAutoincrement
                    };

                    Api.JetAddColumn(session, tableid, "Id", colDef, null, 0, out colid);

                    colDef = new JET_COLUMNDEF()
                    {
                        coltyp = JET_coltyp.Long,
                        grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnEscrowUpdate
                    };

                    byte[] defaultValue = BitConverter.GetBytes(0);
                    Api.JetAddColumn(session, tableid, "AttemptCount", colDef, defaultValue, defaultValue.Length, out colid);

                    colDef = new JET_COLUMNDEF()
                    {
                        coltyp = JET_coltyp.LongBinary,
                    };
                    Api.JetAddColumn(session, tableid, "SerializedObject", colDef, null, 0, out colid);

                    string indexDef;
                    indexDef = "+Id\0\0";
                    Api.JetCreateIndex(session, tableid, "primary", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length, 100);

                    transaction.Commit(CommitTransactionGrbit.None);
                }
            }

        }
    }
}
