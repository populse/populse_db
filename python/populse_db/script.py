##########################################################################
# Populse_db - Copyright (C) IRMaGe/CEA, 2018
# Distributed under the terms of the CeCILL-B license, as published by
# the CEA-CNRS-INRIA. Refer to the LICENSE file or to
# http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html
# for details.
##########################################################################

import os
import tempfile

import shutil

from populse_db.database import Database, FIELD_TYPE_STRING, FIELD_TYPE_INTEGER

# Generating the database in a temp directory
temp_folder = tempfile.mkdtemp()
path = os.path.join(temp_folder, "test.db")
try:
    string_engine = 'sqlite:///' + path
    database = Database(string_engine)

    # Creating the session and working with it
    with database as session:

        # Creating a profile table
        session.add_collection("Profile")

        # Adding several properties
        session.add_field("Profile", "First name", FIELD_TYPE_STRING)
        session.add_field("Profile", "Last name", FIELD_TYPE_STRING)
        session.add_field("Profile", "Age", FIELD_TYPE_INTEGER)

        # Filling the table
        profile1 = {}
        profile1["index"] = "profile1"
        profile1["First name"] = "Jules"
        profile1["Last name"] = "CESAR"
        profile1["Age"] = 55
        session.add_document("Profile", profile1)

        session.add_document("Profile", "profile2")
        session.add_value("Profile", "profile2", "First name", "Louis")
        session.add_value("Profile", "profile2", "Last name", "XIV")
        session.add_value("Profile", "profile2", "Age", 76)

        # Setting a value
        result = session.filter_documents("Profile", "({Age} > 50) AND ({First name} == \"Jules\")")
        for document in result: # profile1 is displayed, as it's the only document with the value Age greater than 50, and the value First name being Jules
            print(document.index)

        session.save_modifications()

finally:

    shutil.rmtree(temp_folder)
