import os
from time import sleep

class Display:
    """
    This class handles everything related to the CLI display
    """

    def __init__(self):
        self.title_bar = """\n***************************************
***********SIS INTERNATIONAL***********
***************************************
"""

        self.user_actions = """Please select one the following options:

    LISTS
    [1] Clean list
    
    [3] Save list to valid_emails database 
    [5] Extract blast needs from valid emails database to mm
    [21] Extract blast needs from valid emails database to blast

    [20] update valid emails against mm log
    
    [5] Send mailmerge
    [6] Block email(s)

    GREEN ARROW
    [2] Import list to GreenArrow
    [4] Send GreenArrow blast

    [7] Create MM list
    [8] BCC MM list
    [9] SM report
    [10] Extract MM lists

    [11] Get daily blast needs

    [12] Mailmerge

    [13] Decompose MM list

    [14] SMTP MM list

    [15] Mailmerge summary

    [16] Get URLs from Snov to Hunt

    [17] Clean list manually

    [18] Concatenate lists

    [19] Deduper

    [22] Divide list

    [23] Clean against email bison

    [Q] Quit
    """
        
    def get_user_choice(self):
        """
        Gets the action to do defined by the user
        """
        print(self.title_bar)
        print(self.user_actions)
        return input("What would you like to do? ")
    
    def quit(self):
        print('Thanks, bye.')
        sleep(1)
        os.system('clear')





        
