using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Isam.Esent.Interop;
using MsgPack.Serialization;

namespace EsentQueue
{
    /// <summary>
    /// A persistent queue with a leasing mechanism to lock-out other threads.
    /// of persistentqueue
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class LeaseQueue<T> : IDisposable
    {
        private const string TableName = "Data";

        private static readonly MessagePackSerializer<T> _serializer = MessagePackSerializer.Get<T>();

        private Instance _instance;
        private string _databaseName;

        public LeaseQueue(string path)
        {
            _instance = new Instance("EsenetQueue.LeaseQueue");
            var fullPath = Path.GetFullPath(path);
            var directory = Path.GetDirectoryName(fullPath) + "\\";
            _instance.Parameters.LogFileDirectory = directory;
            _instance.Parameters.SystemDirectory = directory;
            _instance.Parameters.TempDirectory = directory;
            _instance.Parameters.AlternateDatabaseRecoveryDirectory = directory;
            _instance.Parameters.CreatePathIfNotExist = true;
            _instance.Parameters.CircularLog = true;
            _instance.Parameters.MaxVerPages = 256;
            _instance.Init();
            _databaseName = path;

            CreateDatabase();
        }

        public void Dispose()
        {
            _instance?.Dispose();
        }

        public int Count
        {
            get
            {
                using (var session = new Session(_instance))
                {
                    JET_DBID dbId;
                    Api.OpenDatabase(session, _databaseName, out dbId, OpenDatabaseGrbit.None);
                    using (var table = new Table(session, dbId, TableName, OpenTableGrbit.None))
                    {
                        int count = 0;
                        Api.MoveBeforeFirst(session, table);
                        while (Api.TryMoveNext(session, table))
                        {
                            count++;
                        }

                        return count;
                    }
                }
            }
        }

        public void Enqueue(T item)
        {
            using (var session = new Session(_instance))
            {
                JET_DBID dbId;
                Api.OpenDatabase(session, _databaseName, out dbId, OpenDatabaseGrbit.None);
                using (var table = new Table(session, dbId, TableName, OpenTableGrbit.None))
                {
                    using (var transaction = new Transaction(session))
                    {
                        using (var update = new Update(session, table, JET_prep.Insert))
                        {
                            var objectCol = Api.GetTableColumnid(session, table, "SerializedObject");
                            using (var colStream = new ColumnStream(session, table, objectCol))
                            {
                                _serializer.Pack(colStream, item);
                            }

                            update.Save();
                        }

                        transaction.Commit(CommitTransactionGrbit.LazyFlush);
                    }
                }
            }
        }

        public bool TryPeek(out T item)
        {
            using (var session = new Session(_instance))
            {
                JET_DBID dbId;
                Api.OpenDatabase(session, _databaseName, out dbId, OpenDatabaseGrbit.None);
                using (var transaction = new Transaction(session))
                {
                    using (var table = new Table(session, dbId, TableName, OpenTableGrbit.None))
                    {
                        Api.JetSetCurrentIndex(session, table, "leasetimeout_index");
                        Api.MakeKey(session, table, null, MakeKeyGrbit.NewKey);
                        if (!Api.TrySeek(session, table, SeekGrbit.SeekGE))
                        {
                            item = default(T);
                            return false;
                        }

                        var objectCol = Api.GetTableColumnid(session, table, "SerializedObject");
                        using (var colStream = new ColumnStream(session, table, objectCol))
                        {
                            T obj = _serializer.Unpack(colStream);
                            item = obj;
                        }

                        DebugCheckLeaseNotSet(session, table);
                        return true;
                    }
                }
            }
        }

        public bool TryTakeLease(out QueueItemLease<T> lease)
        {
            using (var session = new Session(_instance))
            {
                JET_DBID dbId;
                Api.OpenDatabase(session, _databaseName, out dbId, OpenDatabaseGrbit.None);

                using (var transaction = new Transaction(session))
                {
                    using (var table = new Table(session, dbId, TableName, OpenTableGrbit.None))
                    {
                        Api.JetSetCurrentIndex(session, table, "leasetimeout_index");
                        Api.MakeKey(session, table, null, MakeKeyGrbit.NewKey | MakeKeyGrbit.FullColumnStartLimit);

                        if (!Api.TrySeek(session, table, SeekGrbit.SeekGE))
                        {
                            lease = default(QueueItemLease<T>);
                            return false;
                        }

                        Api.MakeKey(session, table, null, MakeKeyGrbit.NewKey | MakeKeyGrbit.FullColumnEndLimit);
                        if (!Api.TrySetIndexRange(session, table, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit))
                        {
                            lease = default(QueueItemLease<T>);
                            return false;
                        }

                        T item;
                        var bookmark = Api.GetBookmark(session, table);
                        var leaseCol = Api.GetTableColumnid(session, table, "LeaseTimeout");
                        var objectCol = Api.GetTableColumnid(session, table, "SerializedObject");
                        using (var colStream = new ColumnStream(session, table, objectCol))
                        {
                            item = _serializer.Unpack(colStream);
                        }

                        using (var update = new Update(session, table, JET_prep.Replace))
                        {
                            Api.SetColumn(session, table, leaseCol, (DateTime.Now + TimeSpan.FromSeconds(5)).Ticks);
                            update.Save();
                        }

                        transaction.Commit(CommitTransactionGrbit.LazyFlush);
                        lease = new QueueItemLease<T>(this, item, bookmark);
                        return true;
                    }
                }
            }
        }

        internal void RemoveAtBookmark(byte[] bookmark)
        {
            using (var session = new Session(_instance))
            {
                JET_DBID dbId;
                Api.OpenDatabase(session, _databaseName, out dbId, OpenDatabaseGrbit.None);

                using (var transaction = new Transaction(session))
                {
                    using (var table = new Table(session, dbId, TableName, OpenTableGrbit.None))
                    {
                        if (Api.TryGotoBookmark(session, table, bookmark, bookmark.Length))
                        {
                            Api.JetDelete(session, table);
                            transaction.Commit(CommitTransactionGrbit.LazyFlush);
                        }
                    }
                }
            }
        }

        internal void Rollback(byte[] bookmark)
        {
            using (var session = new Session(_instance))
            {
                JET_DBID dbId;
                Api.OpenDatabase(session, _databaseName, out dbId, OpenDatabaseGrbit.None);

                using (var transaction = new Transaction(session))
                {
                    using (var table = new Table(session, dbId, TableName, OpenTableGrbit.None))
                    {
                        if (Api.TryGotoBookmark(session, table, bookmark, bookmark.Length))
                        {
                            var leaseCol = Api.GetTableColumnid(session, table, "LeaseTimeout");

                            using (var update = new Update(session, table, JET_prep.Replace))
                            {
                                Api.SetColumn(session, table, leaseCol, null);
                                update.Save();
                            }

                            transaction.Commit(CommitTransactionGrbit.LazyFlush);
                        }
                    }
                }
            }
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
                    Api.JetCreateTable(session, dbid, TableName, 16, 100, out tableid);

                    colDef = new JET_COLUMNDEF()
                    {
                        coltyp = JET_coltyp.Long,
                        grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnAutoincrement
                    };

                    Api.JetAddColumn(session, tableid, "Id", colDef, null, 0, out colid);

                    colDef = new JET_COLUMNDEF()
                    {
                        coltyp = JET_coltyp.LongBinary,
                    };
                    Api.JetAddColumn(session, tableid, "SerializedObject", colDef, null, 0, out colid);

                    colDef = new JET_COLUMNDEF()
                    {
                        coltyp = JET_coltyp.Currency,
                    };
                    Api.JetAddColumn(session, tableid, "LeaseTimeout", colDef, null, 0, out colid);

                    string indexDef;
                    indexDef = "+Id\0\0";
                    Api.JetCreateIndex(session, tableid, "primary", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length, 100);

                    indexDef = "+LeaseTimeout\0+Id\0\0";
                    Api.JetCreateIndex(session, tableid, "leasetimeout_index", CreateIndexGrbit.None, indexDef, indexDef.Length, 100);

                    transaction.Commit(CommitTransactionGrbit.None);
                }
            }
        }

        [Conditional("Debug")]
        private static void DebugCheckLeaseNotSet(Session session, Table table)
        {
            var col = Api.GetTableColumnid(session, table, "LeaseTimeout");
            long? value = Api.RetrieveColumnAsInt64(session, table, col);
            Debug.Assert(!value.HasValue, $"the lease timeout value is {value}");
        }
    }

    public class QueueItemLease<T>
    {
        private LeaseQueue<T> _queue;
        private byte[] _bookmark;

        internal QueueItemLease(LeaseQueue<T> queue, T item, byte[] bookmark)
        {
            _queue = queue;
            Item = item;
            _bookmark = bookmark;
        }

        public T Item { get; }

        public void MarkCompleted()
        {
            _queue.RemoveAtBookmark(_bookmark);
        }

        public void Rollback()
        {
            _queue.Rollback(_bookmark);
        }
    }
}