import datetime
import sys

class SlowLogEntry:
    def __init__(self, entry, time, execute, command, client_socket, client_name):

        self.entry_no = entry
        self.timestamp = time
        self.time_to_execute = execute
        self.command = command

        ## if version >=4
        self.client_socket = client_socket
        assert isinstance(client_name, str)
        self.client_name = client_name

    def get(self, time='unix'):
        print("Entry No ( Not much relevance ): " + str(self.entry_no))
        print("Timestamp: ", end = "")
        if time.strip() == "UTC" or time.strip() == "utc":
            print(datetime.datetime.utcfromtimestamp(self.timestamp).strftime('%d-%m-%Y T %H:%M:%S UTC'))
        else:
            print(self.timestamp)
        print("Time to execute in microseconds: " + str(self.time_to_execute))
        print("Command Array: \n\t" + '\n\t'.join(self.command))
        print("Client Address: ", end = "")
        print( "Not Supported in redis version less than 4.0" if self.client_socket == '' else self.client_socket )
        print("Client Name: ", end = "" )
        if self.client_socket == '':
            print ("Not Supported in redis version less than 4.0")
        print(self.client_name)
        print("\n------------End of Entry--------------\n")



class SlowLogParser:

    def __init__(self, data):
        self.text = data
        self.raw_parsed = []
        self.parsed = []

    @staticmethod
    def __number_of_head_spaces(line):
        c = 0
        for i in line:
            if i != ' ':
                break
            else:
                c = c + 1
        return c

    @staticmethod
    def __first_bracket_pos(line):
        c = 0
        for i in line:
            if i == ')':
                break
            else:
                c = c + 1
        return c + 1

    def __determine_mid_width(self):
        l = len(self.text)
        c = l - 1
        while self.__number_of_head_spaces(self.text[c]) != 0:
            c -= 1
        return self.__number_of_head_spaces(self.text[c+1])

    def __get_parsing_in_object(self):
        if len(self.raw_parsed) == 0:
            print ("Error: Empty parsing")
        else:
            for item in self.raw_parsed[::-1]:
                reversed_item = item[::-1]
                temp_store_for_entry = SlowLogEntry(
                    int(reversed_item[0].split("(integer)")[1].strip()),
                    int(reversed_item[1].split("(integer)")[1].strip()),
                    int(reversed_item[2].split("(integer)")[1].strip()),
                    [ elem[self.__first_bracket_pos(elem):].strip() for elem in reversed_item[3] ],
                    '' if len(item) < 4 else reversed_item[4].split(')')[1].strip(),
                    '' if len(item) < 4 else reversed_item[5].split(')')[1].strip()
                )
                self.parsed.append(temp_store_for_entry)

    def __parse_helper(self):
        self.raw_parsed = []
        mid_width = self.__determine_mid_width()
        line_count = len(self.text) - 1
        element = []
        mid_element = []

        while line_count >= 0 :
            current_line = self.text[line_count]
            count_of_head_spaces = self.__number_of_head_spaces(current_line)
            if count_of_head_spaces == mid_width :
                if current_line[count_of_head_spaces] == '4':
                    first_bracket_position = self.__first_bracket_pos(current_line)
                    mid_element.append(current_line[first_bracket_position:].strip())
                    element.append(mid_element[::-1])
                else:
                    element.append(current_line.strip())
            elif count_of_head_spaces > mid_width :
                mid_element.append(current_line.strip())
            else:
                first_bracket_position = self.__first_bracket_pos(current_line)
                element.append(current_line[first_bracket_position:].strip())
                self.raw_parsed.append(element)
                element = []
                mid_element = []
            line_count -=1

        """
        ## Only for DEBUG
        for i in self.raw_parsed[::-1]:
            print(i[::-1])
        """

    def parse(self,time=''):
        ## error checks
        if  len( self.text ) == 0 or self.text[0].strip() == "(empty list or set)":
            print("ERROR: Logs are empty")
            return

        ## parsing the SlowLog
        self.__parse_helper()

        ## testing the length
        test_item = self.raw_parsed[0]
        if len ( test_item ) != 4 and len ( test_item ) != 6:
            print("ERROR: Log start is in Invalid Format. Expected Length: 4 or 6. Length: " + str(len(test_item)))
            return

        base_len = len( test_item )

        for item in range(len(self.raw_parsed)):
             if len(self.raw_parsed[item]) != base_len:
                 print("ERROR: Log item " + str(item+1) + " is in Invalid Format. Expected Length: " + str(base_len) + ".  Length: " + len(self.raw_parsed[item]))
                 return

        ## create objects
        self.__get_parsing_in_object()

        ## display entry
        for entry in self.parsed:
            entry.get(time)


class FileReader:
    def __init__(self, filename):
        f = open(filename, "r")
        self.data = f.readlines()

    def read(self):
        return self.data

"""
if __name__ == "__main__":
    r = FileReader('SlowLog')
    p = SlowLogParser(r.read())
    p.parse(time='utc')
"""    
    
USAGE = """
    Supports only Python 3
    Format:
    $ python3 SlowLogReader.py  <FileName>  <TS: Blank or UTC>

"""

if __name__ == "__main__":
    params = sys.argv
    if len(params) < 2 and len(params) > 3:
        print (USAGE)
        exit(1)
    r = FileReader(params[1])
    p = SlowLogParser(r.read())
    time = ''
    if len(params) > 1 and params[2] == 'utc' or params[2] == "UTC":
        time = 'utc'
    p.parse(time=time)
