from populse_db.database import Database
from populse_db.database_model import COLUMN_TYPE_STRING, COLUMN_TYPE_FLOAT, COLUMN_TYPE_INTEGER, COLUMN_TYPE_LIST_FLOAT
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

    database = Database(string_engine, True, True)

    columns = []

    for i in range(0, 20):
        columns.append(["column" + str(i), COLUMN_TYPE_FLOAT, None])
    for i in range(20, 40):
        columns.append(["column" + str(i), COLUMN_TYPE_STRING, None])
    for i in range(40, 50):
        columns.append(["column" + str(i), COLUMN_TYPE_INTEGER, None])
    for i in range(50, 75):
        columns.append(["column" + str(i), COLUMN_TYPE_LIST_FLOAT, None])

    database.add_columns(columns)

    current_documents = database.get_documents_names()

    for i in range(0, 1000):
        document_name = "document" + str(i)
        if not document_name in current_documents:
            database.add_document(document_name, False)
    database.session.flush()

    for i in range(0, 1000):
        document_name = "document" + str(i)
        for j in range(0, 20):
            if document_name in current_documents:
                database.remove_value(document_name, "column" + str(j), False)
            database.new_value(document_name, "column" + str(j), 1.5, 1.5, False)
        for j in range(20, 40):
            if document_name in current_documents:
                database.remove_value(document_name, "column" + str(j), False)
            database.new_value(document_name, "column" + str(j), "value", "value", False)
        for j in range(40, 50):
            if document_name in current_documents:
                database.remove_value(document_name, "column" + str(j), False)
            database.new_value(document_name, "column" + str(j), 5, 5, False)
        for j in range(50, 75):
            if document_name in current_documents:
                database.remove_value(document_name, "column" + str(j), False)
            database.new_value(document_name, "column" + str(j), [1, 2, 3], [1, 2, 3], False)
    database.session.flush()

    simple_search = database.get_documents_matching_search("1", ["column0", "column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9"])
    print(simple_search) # All paths

    simple_search = database.get_documents_matching_search("1.2",
                                                       ["column0", "column1", "column2", "column3", "column4", "column5", "column6", "column7",
                                                        "column8", "column9"])
    print(simple_search) # No document

    simple_search = database.get_documents_matching_search("1.5",
                                                       ["column0", "column1", "column2", "column3", "column4", "column5", "column6", "column7",
                                                        "column8", "column9"])
    print(simple_search) # All documents

    advanced_search = database.get_documents_matching_advanced_search([], [["column1"]], ["="], ["1.5"], [""],
                                                              ["document0", "document1", "document2", "document3", "document4", "document5", "document6", "document7", "document8", "document9"])
    print(advanced_search) # 10 first columns

    shutil.rmtree(temp_folder)

    print("--- %s seconds ---" % (time.time() - start_time))

    #prof.print_stats()