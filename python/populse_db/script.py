from populse_db.database import Database
from populse_db.database_model import FIELD_TYPE_STRING, FIELD_TYPE_FLOAT, FIELD_TYPE_INTEGER, FIELD_TYPE_LIST_FLOAT
import os
import tempfile
import shutil
import time
import pprofile

if __name__ == '__main__':

    #import pprofile
    #prof = pprofile.Profile()
    #with prof():

    start_time = time.time()

    temp_folder = tempfile.mkdtemp()
    path = os.path.join(temp_folder, "test.db")
    string_engine = 'sqlite:///' + path

    database = Database(string_engine, True)

    fields = []

    current = "current"
    initial = "initial"

    database.add_collection(current, "FileName")
    database.add_collection(initial, "FileName")

    for i in range(0, 20):
        fields.append([current, "field" + str(i), FIELD_TYPE_FLOAT, None])
        fields.append([initial, "field" + str(i), FIELD_TYPE_FLOAT, None])
    for i in range(20, 40):
        fields.append([current, "field" + str(i), FIELD_TYPE_STRING, None])
        fields.append([initial, "field" + str(i), FIELD_TYPE_STRING, None])
    for i in range(40, 50):
        fields.append([current, "field" + str(i), FIELD_TYPE_INTEGER, None])
        fields.append([initial, "field" + str(i), FIELD_TYPE_INTEGER, None])
    for i in range(50, 75):
        fields.append([current, "field" + str(i), FIELD_TYPE_LIST_FLOAT, None])
        fields.append([initial, "field" + str(i), FIELD_TYPE_LIST_FLOAT, None])

    database.add_fields(fields)

    current_documents = database.get_documents_names(current)

    for i in range(0, 1000):
        document_name = "document" + str(i)
        if not document_name in current_documents:
            database.add_document(current, document_name, False)
            database.add_document(initial, document_name, False)
    database.__session.flush()

    for i in range(0, 1000):
        document_name = "document" + str(i)
        for j in range(0, 20):
            if document_name in current_documents:
                database.remove_value(document_name, "field" + str(j), False)
            database.new_value(current, document_name, "field" + str(j), 1.5, False)
            database.new_value(initial, document_name, "field" + str(j), 1.5, False)
        for j in range(20, 40):
            if document_name in current_documents:
                database.remove_value(document_name, "field" + str(j), False)
            database.new_value(current, document_name, "field" + str(j), "value", False)
            database.new_value(initial, document_name, "field" + str(j), "value", False)
        for j in range(40, 50):
            if document_name in current_documents:
                database.remove_value(document_name, "field" + str(j), False)
            database.new_value(current, document_name, "field" + str(j), 5, False)
            database.new_value(initial, document_name, "field" + str(j), 5, False)
        for j in range(50, 75):
            if document_name in current_documents:
                database.remove_value(document_name, "field" + str(j), False)
            database.new_value(current, document_name, "field" + str(j), [1, 2, 3], False)
            database.new_value(initial, document_name, "field" + str(j), [1, 2, 3], False)
    database.__session.flush()

    print("--- %s seconds ---" % (time.time() - start_time))

    #prof.print_stats()