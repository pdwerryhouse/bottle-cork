
import sys
from logging import getLogger

from . import base_backend

log = getLogger(__name__)

CQL_SIMPLE = 1
CQL_NETWORK_TOPOLOGY = 2

try:
    from cassandra.cqlengine import columns, connection
    from cassandra.cqlengine.models import Model
    from cassandra.cqlengine.management import sync_table, drop_table
    from cassandra.cqlengine.management import create_keyspace_network_topology
    from cassandra.cqlengine.management import create_keyspace_simple
    from cassandra.cqlengine.connection import get_session
    cqlengine_available = True
except ImportError:
    cqlengine_available = False

class User(Model):
    username = columns.Text(primary_key=True)
    role = columns.Text()
    hash = columns.Text()
    email_addr = columns.Text()
    desc = columns.Text()
    creation_date = columns.Text()
    last_login = columns.Text()

class Role(Model):
    role = columns.Text(primary_key=True)
    level = columns.Integer()

class PendingRegistrationUser(Model):
    pending_reg_id = columns.Text(primary_key=True)
    code = columns.Text(index=True)
    username = columns.Text(index=True)
    role = columns.Text()
    hash = columns.Text()
    email_addr = columns.Text()
    desc = columns.Text()
    creation_date = columns.Text()
    last_login = columns.Text()

class CqlRowProxy(dict):
    def __init__(self, sql_dict, key, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.sql_dict = sql_dict
        self.key = key

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        if self.sql_dict is not None:
            self.sql_dict[self.key] = {key: value}


class CqlTable(base_backend.Table):
    """Provides dictionary-like access to a CQL table."""

    def __init__(self, table):
        self._table = table
        self._key_col = table.pk
        self._key_col_name = table.pk.column.db_field_name

    def _row_to_value(self, row):
        row_key = row.pk
        row_value = CqlRowProxy(self, row_key,
            ((k, row[k]) for k in row.keys() if k != self._key_col_name))

        return row_key, row_value

    def __len__(self):
        c = self._table.all().count()
        return c

    def __contains__(self, key):
        c = self._table.objects(self._key_col == key)
        return c.count() is not 0

    def __setitem__(self, key, value):
        if key in self:
            row = self._table.objects(self._key_col == key).first()
            for k, v in value.iteritems():
                row[k] = v
            row.update()

        else:
            thing = self._table()
            thing[self._key_col_name] = key
            for k, v in value.iteritems():
                thing[k] = v
            thing.save()


    def __getitem__(self, key):
        row = self._table.objects(self._key_col == key).first()
        if row is None:
            raise KeyError(key)
        return self._row_to_value(row)[1]

    def __iter__(self):
        """Iterate over table index key values"""
        for row in self._table.objects.all():
            yield row.pk

    def iteritems(self):
        """Iterate over table rows"""
        for row in self._table.objects.all():
            key = row.pk
            d = self._row_to_value(row)[1]
            yield (key, d)
            
    def pop(self, key):
        row = self._table.objects(self._key_col == key).first()
        if row is None:
            raise KeyError

        row.delete()
        return row

    def insert(self, d):
        thing = self._table()
        keys = thing.keys()
        for i in range(0,len(keys)):
            thing[keys[i]] = d[i]
        thing.save()
        log.debug("%s inserted" % repr(d))

    def empty_table(self):
        session = get_session()
        session.execute("TRUNCATE %s.%s" % (self._table._get_keyspace(), self._table._table_name))
        log.info("Table purged")

class CqlSingleValueTable(CqlTable):
    def __init__(self, table, col_name):
        CqlTable.__init__(self, table)
        self._col_name = col_name

    def _row_to_value(self, row):
        return row[self._key_col_name], row[self._col_name]

    def __setitem__(self, key, value):
        CqlTable.__setitem__(self, key, {self._col_name: value})

class CqlEngineBackend(base_backend.Backend):

    def __init__(self, servers=[], keyspace="", username="", password="", 
            users_tname='users', roles_tname='roles',
            pending_reg_tname='register', initialize=False,
            replication_type = CQL_SIMPLE, replication_factor_map = None,  **kwargs):

        if not cqlengine_available:
            raise RuntimeError("The cqlengine library is not available.")

        connection.setup(servers, keyspace)

        if initialize:
            self._initialize_storage(keyspace, replication_type, replication_factor_map)
            log.debug("Keyspace created")


        User.__table_name__ = users_tname;
        Role.__table_name__ = roles_tname;
        PendingRegistrationUser.__table_name__ = pending_reg_tname;

        sync_table(User)
        sync_table(Role)
        sync_table(PendingRegistrationUser)

        self.users = CqlTable(User)
        self.roles = CqlSingleValueTable(Role,'level')
        self.pending_registrations = CqlTable(PendingRegistrationUser)


    def _initialize_storage(self, keyspace, replication_type, replication_factor_map):
        if replication_type == CQL_SIMPLE:
            create_keyspace_simple(keyspace, replication_factor_map)
        elif replication_type == CQL_NETWORK_TOPOLOGY:
            create_keyspace_simple(keyspace, replication_factor_map)
        else:
            raise NotImplementedError

    def _drop_all_tables(self):
        drop_table(User)
        drop_table(Role)
        drop_table(PendingRegistrationUser)

    def save_users(self): pass
    def save_roles(self): pass
    def save_pending_registrations(self): pass

