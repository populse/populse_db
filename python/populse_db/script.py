from populse_db.database import Database
from populse_db.database_model import TAG_ORIGIN_BUILTIN, TAG_TYPE_STRING, TAG_TYPE_FLOAT, TAG_TYPE_INTEGER, TAG_TYPE_LIST_FLOAT
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

    current_paths = database.get_paths_names()

    for i in range(0, 1000):
        path_name = "path" + str(i)
        if not path_name in current_paths:
            database.add_path(path_name)
        for j in range(0, 20):
            if path_name in current_paths:
                database.remove_value(path_name, "tag" + str(j), False)
            database.new_value(path_name, "tag" + str(j), 1.5, 1.5, False)
        for j in range(20, 40):
            if path_name in current_paths:
                database.remove_value(path_name, "tag" + str(j), False)
            database.new_value(path_name, "tag" + str(j), "value", "value", False)
        for j in range(40, 50):
            if path_name in current_paths:
                database.remove_value(path_name, "tag" + str(j), False)
            database.new_value(path_name, "tag" + str(j), 5, 5, False)
        for j in range(50, 75):
            if path_name in current_paths:
                database.remove_value(path_name, "tag" + str(j), False)
            database.new_value(path_name, "tag" + str(j), [1, 2, 3], [1, 2, 3], False)
        database.session.flush()

    """
    simple_search = database.get_paths_matching_search("1", ["tag0", "tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7", "tag8", "tag9"])
    print(simple_search) # All paths

    simple_search = database.get_paths_matching_search("1.2",
                                                       ["tag0", "tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7",
                                                        "tag8", "tag9"])
    print(simple_search) # No path

    simple_search = database.get_paths_matching_search("1.5",
                                                       ["tag0", "tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7",
                                                        "tag8", "tag9"])
    print(simple_search) # All paths

    advanced_search = database.get_paths_matching_advanced_search([], [["tag1"]], ["="], [1.5], [""],
                                                              ["path0", "path1", "path2", "path3", "path4", "path5", "path6", "path7", "path8", "path9"])
    print(advanced_search) # 10 first paths
    """

    shutil.rmtree(temp_folder)

    print("--- %s seconds ---" % (time.time() - start_time))

    #prof.print_stats()