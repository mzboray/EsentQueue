using System;
using System.Collections.Generic;
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
    /// This is mostly just for testing out concepts in esent without the complexity
    /// of persistentqueue
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class TestQueue<T> : IDisposable
    {
        private static readonly MessagePackSerializer<T> _serializer = MessagePackSerializer.Get<T>();

        private Instance _instance;
        private string _databaseName;

        public TestQueue(string path)
        {
            _instance = new Instance("queue");
            _instance.Parameters.CircularLog = true;
            _instance.Init();
            _databaseName = path;

            CreateDatabase();
        }

        public void Dispose()
        {
            _instance?.Dispose();
        }

        public void Enqueue(T item)
        {
            using (var session = new Session(_instance))
            {
                JET_DBID dbId;
                Api.OpenDatabase(session, _databaseName, out dbId, OpenDatabaseGrbit.None);
                using (var table = new Table(session, dbId, "Data", OpenTableGrbit.None))
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

        public bool TryDequeue(out T item)
        {
            using (var session = new Session(_instance))
            {
                JET_DBID dbId;
                Api.OpenDatabase(session, _databaseName, out dbId, OpenDatabaseGrbit.None);

                using (var transaction = new Transaction(session))
                {
                    using (var table = new Table(session, dbId, "Data", OpenTableGrbit.None))
                    {
                        Api.MoveBeforeFirst(session, table);
                        while (true)
                        {
                            if (!Api.TryMoveNext(session, table))
                            {
                                item = default(T);
                                return false;
                            }

                            if (Api.TryGetLock(session, table, GetLockGrbit.Write))
                            {
                                break;
                            }
                        }

                        var objectCol = Api.GetTableColumnid(session, table, "SerializedObject");
                        using (var colStream = new ColumnStream(session, table, objectCol))
                        {
                            T obj = _serializer.Unpack(colStream);
                            item = obj;
                        }

                        Api.JetDelete(session, table);
                        transaction.Commit(CommitTransactionGrbit.LazyFlush);
                        return true;
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
                    Api.JetCreateTable(session, dbid, "Data", 16, 100, out tableid);

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

                    string indexDef;
                    indexDef = "+Id\0\0";
                    Api.JetCreateIndex(session, tableid, "primary", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length, 100);

                    transaction.Commit(CommitTransactionGrbit.None);
                }
            }
        }
    }
}
