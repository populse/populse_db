import os
import tempfile

import shutil

from populse_db.database import Database, FIELD_TYPE_STRING, FIELD_TYPE_INTEGER

# Generating the database in a temp directory
temp_folder = tempfile.mkdtemp()
path = os.path.join(temp_folder, "test.db")
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
    profile1["name"] = "profile1"
    profile1["First name"] = "Lucie"
    profile1["Last name"] = "OUVRIER-BUFFET"
    profile1["Age"] = "23"
    session.add_document("Profile", profile1)

    session.add_document("Profile", "profile2")
    session.add_value("Profile", "profile2", "First name", "David")
    session.add_value("Profile", "profile2", "Last name", "HARBINE")
    session.add_value("Profile", "profile2", "Age", 23)

    session.save_modifications()

shutil.rmtree(temp_folder)