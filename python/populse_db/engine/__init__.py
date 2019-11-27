
def engine_factory(database_url):
    if database_url.startswith('sqlite:///'):
        from .sqlite import SQLiteEngine
        return SQLiteEngine(database_url[10:])
    else:
        raise ValueError('Invalid database URL: %s' % database_url)


class Engine:
    def __init__(self, database):
        raise NotImplementedError()


    def __enter__(self):
        raise NotImplementedError()

    
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError()

    
    def clear(self):
        raise NotImplementedError()

        
    def has_collection(self, collection):
        raise NotImplementedError()


    def add_collection(self, collection, primary_key):
        raise NotImplementedError()

        
    def collection(self, collection):
        raise NotImplementedError()

    
    def primary_key(self, collection):
        raise NotImplementedError()

    
    def remove_collection(self, collection):
        raise NotImplementedError()


    def collections(self):        
        raise NotImplementedError()


    def add_field(self, collection, field, type, description, index):
        raise NotImplementedError()

            
    def has_field(self, collection, field):
        raise NotImplementedError()

    
    def field(self, collection, field):
        raise NotImplementedError()


    def fields(self, collection=None):
        raise NotImplementedError()

    
    def remove_fields(self, collection ,fields):
        raise NotImplementedError()

    
    def has_document(self, collection, document):
        raise NotImplementedError()


    def document(self, collection, document):
        raise NotImplementedError()

    
    def get_value(self, collection, document_id, field):
        raise NotImplementedError()

        
    def set_values(self, collection, document_id, values):
        raise NotImplementedError()


    def remove_value(self, collection, document_id, field):
        raise NotImplementedError()
        

    def remove_document(self, collection, document_id):
        raise NotImplementedError()

        
    def parse_filter(self, collection, filter):
        """
        Given a filter string, return a internal query representation that
        can be used with filter_documents() to select documents


        :param collection: the collection for which the filter is intended 
               (str, must be existing)
        
        :param filter: the selection string using the populse_db selection
                       language.

        """
        raise NotImplementedError()


    def filter_documents(self, parsed_filter):
        raise NotImplementedError()

