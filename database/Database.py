class Database:

    def __init__(self):
        pass

    """ TAGS """

    def add_tag(self, tag, visible, origin, type, unit, default_value, description):
        pass

    def remove_tag(self, tag):
        pass

    """ VALUES """

    def get_current_value(self, scan, tag):
        pass

    def get_initial_value(self, scan, tag):
        pass

    def is_value_modified(self, scan, tag):
        pass

    def set_value(self, scan, tag, new_value):
        pass

    def reset_value(self, scan, tag):
        pass

    def remove_value(self, scan, tag):
        pass

    def add_value(self, scan, tag, value):
        pass

    """ SCANS """

    def remove_scan(self, scan):
        pass

    def add_scan(self, scan, checksum):
        pass