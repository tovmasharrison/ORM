import sqlite3
from sqlite3 import Error


class Field:
    """ The Field class is a basic implementation of a class attribute
        that can hold a value of a certain type. It has methods to set 
        and get the value, as well as a default value if one is not specified. 
        When assigned to a class attribute, it stores the name of the attribute. """

    def __init__(self, field_type, default = None):
        self.field_type = field_type
        self.default = default
        self.value = None

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self.value

    def __set__(self, instance, value):
        self.value = value


class Storage:
    """ The Storage class is a manager for the ORM that allows interaction 
        with a data store for a specific class. Its constructor initializes 
        the object and sets the name attribute to the name of the class. """

    def __init__(self, cls):
        self.cls = cls       
        self.name = cls.__name__

    @property
    def conn(self):
        """ Establishes the connection to the database """

        con = None
        try:
            con = sqlite3.connect(f"{self.name}.db")
            print("Connected Successfuly")
            return con
        except Error as e:
            print(f"The error {e} occured")

    @staticmethod
    def execute_query(connection, query, *attrs):
        """ Executes the query """

        cursor = connection.cursor()
        try:
            cursor.execute(query, *attrs) 
            connection.commit()
            connection.close()
            print("Query executed successfully")
        except Error as e:
            print(f"The error '{e}' occured")

    @staticmethod
    def execute_read_query(connection, query, *attrs):
        """ Reads the given query """

        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query, *attrs)
            result = cursor.fetchall()
            for res in result:
                print(res) 
        except Error as e:
            print(f"The error '{e}' occured")

    def create(self):
        """ Creates database from form fields """

        field_types = []
        for field in self.cls.fields:
            field_definition = f"{field.name} {field.field_type}"
            if field.default is not None:
                if field.default == "":
                    field_definition += " DEFAULT '' "
                else:
                    field_definition += f" DEFAULT {field.default}"
            field_types.append(field_definition)
        query = f"CREATE TABLE IF NOT EXISTS {self.cls.__name__} ({', '.join(field_types)})"
        self.execute_query(self.conn, query)
        print("Table Created Successfully")
   
    def read(self):
        """ Outputs all data from the database """ 

        query = f"SELECT * FROM {self.name}"
        return self.execute_read_query(self.conn, query)
            
    def update(self, id, **kwargs):
        """ Updates the base data according to the received argument and the matching condition """

        values = []
        updated = []  #conditions
        for field, value in kwargs.items():
            updated.append(f"{field} = ?")
            values.append(value)
        updated = ", ".join(updated)
        query = f"UPDATE {self.name} SET {updated} WHERE id = ?"
        values.append(id)
        self.execute_query(self.conn, query, values)

    def delete(self, **kwargs):
        """Deletes the data from the database according to the received argument and corresponding condition.
           If none given, deletes the whole database """

        if kwargs:
            conditions = []
            values = []
            for field,value in kwargs.items():
                conditions.append(f"{field} = ?")
                values.append(value)
            query = f"DELETE FROM {self.name} WHERE {' AND '.join(conditions)}"
            self.execute_query(self.conn, query, values)
            print(f"{','.join(values)} has been deleted successfully")
        query = f"DROP TABLE {self.name}"
        self.execute_query(self.conn, query)
        print(f"{self.name} table has been dropped")
       
    def insert(self, **kwargs):
        """ Inserts the data into the database and sets the values of the Field objects 
            in the instance based on the keyword arguments passed to the insert method """
        
        for field_name, value in kwargs.items():
            field = getattr(self.cls, field_name)
            field.__set__(self, value)
        columns = []
        values = []
        for column,value in kwargs.items():
            columns.append(column)
            values.append(value)
        query = f"INSERT INTO {self.name}({','.join(columns)}) VALUES ({','.join(['?'] * (len(columns)))})"
        self.execute_query(self.conn, query, values)
        print("Inserted successfully")
            
    def get_by_field(self, **kwargs):
        """Gets the data from the database according to the condition."""

        values = []
        conditions = []
        for field, value in kwargs.items():
            conditions.append(f"{field} = ?")
            values.append(value)
        query = f"SELECT * FROM {self.name} WHERE {' AND '.join(conditions)}"
        return self.execute_read_query(self.conn, query, values)


class BaseMeta(type):
    """ A metaclass that adds the fields attribute to the class with a list of all Field objects 
        found in the class attributes and creates a Storage object for the class """

    def __new__(cls, name, bases, attrs: dict):    
        fields = []
        for value in attrs.values():
            if isinstance(value, Field):
                fields.append(value)
        attrs["fields"] = fields
        new_cls = super().__new__(cls, name, bases, attrs)
        new_cls.storage = Storage(new_cls)   
        return new_cls
    
    
class BaseModel(metaclass = BaseMeta):
    """ Creates the database when an instance of its subclass created """

    def __init__(self):
        self.storage.create()
        
        

if __name__ == "__main__":
     

    class User(BaseModel):
        id = Field('INTEGER PRIMARY KEY AUTOINCREMENT')
        age = Field('INTEGER')
        first_name = Field('VARCHAR(30)', default = '')
        last_name = Field('VARCHAR(30)', default = '')

        

    def testing():
        #create
        print('\n#create')
        user = User()
        print('\n')
        
        #insert
        print('#insert')
        user.storage.insert(age = 19, first_name = 'John', last_name = 'Doe')
        user.storage.insert(age = 28, first_name = 'Magnus', last_name = 'Carlsen')
        user.storage.insert(age = 29, first_name = 'Hikaru', last_name = 'Nakamura')
        user.storage.insert(age = 60, first_name = 'Gary', last_name = 'Kasparov')
        user.storage.insert(age = 30, first_name = 'Levon')
        print('\n')
        
        #read
        print('#read')
        user.storage.read()
        print(user.age)
        print('\n')
        
        #update
        print('#update')
        user.storage.update(id=3,last_name = 'Tigran', first_name = 'Petrosian')
        user.storage.read()
        print('\n')
        
        #delete
        # print('#delete')
        # user.storage.delete(first_name = 'Magnus')
        # user.storage.delete(last_name = 'Magnus')
        # user.storage.delete() 
        # user.storage.read()
        print('\n')
        
        #get_by_fielld
        print('#get_by_field')
        user.storage.get_by_field(last_name = "Hikaru")
        user.storage.get_by_field(last = "Hikaru")
        user.storage.get_by_field(last_name = "Hikaru", first_name = 'Magnus')
        print('\n')


        
        return True

    testing()


