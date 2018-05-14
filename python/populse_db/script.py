from populse_db.database import Database
from populse_db.database_model import TAG_ORIGIN_BUILTIN, TAG_TYPE_STRING, TAG_TYPE_FLOAT, TAG_TYPE_INTEGER, TAG_TYPE_LIST_FLOAT
import os
import tempfile
import shutil
import time
import pprofile

if __name__ == '__main__':

    start_time = time.time()

    temp_folder = tempfile.mkdtemp()
    path = os.path.join(temp_folder, "test.db")
    string_engine = 'sqlite:///' + path

    database = Database(string_engine)

    tags = []

    for i in range(0, 20):
        tags.append(["tag" + str(i), TAG_ORIGIN_BUILTIN, TAG_TYPE_FLOAT, None, None,
                     None])
    for i in range(20, 40):
        tags.append(["tag" + str(i), TAG_ORIGIN_BUILTIN, TAG_TYPE_STRING, None, None,
                     None])
    for i in range(40, 50):
        tags.append(["tag" + str(i), TAG_ORIGIN_BUILTIN, TAG_TYPE_INTEGER, None, None,
                     None])
    for i in range(50, 75):
        tags.append(["tag" + str(i), TAG_ORIGIN_BUILTIN, TAG_TYPE_LIST_FLOAT, None, None,
                     None])

    database.add_tags(tags)

    for i in range(0, 200):
        database.add_path("scan" + str(i))
        for j in range(0, 20):
            database.new_value("scan" + str(i), "tag" + str(j), 1.5, 1.5)
        for j in range(20, 40):
            database.new_value("scan" + str(i), "tag" + str(j), "value", "value")
        for j in range(40, 50):
            database.new_value("scan" + str(i), "tag" + str(j), 5, 5)
        for j in range(50, 75):
            database.new_value("scan" + str(i), "tag" + str(j), [1, 2, 3], [1, 2, 3])

    shutil.rmtree(temp_folder)

    print("--- %s seconds ---" % (time.time() - start_time))