import luigi
import sqlite3
import csv
import logging
import datetime as dt
import sys
from SQL.CutsheetSQL import INSERT_TASK_LOG, INSERT_CUTSHEET_STG
from Helpers.SQLiteTarget import TaskLogTarget

current_date_nk = dt.datetime.now()
current_date_nk = current_date_nk.strftime("%Y%m%d")
logger = logging.getLogger('cutsheet')
hdlr = logging.FileHandler('Logs/cutsheet_{}.log'.format(current_date_nk))
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


class ExtractCSV(luigi.Task):
    # luigi accepts command line parameters to tasks using the luigi.Parameter() method
    # There are plenty of these like DataParameter() BoolParameter() for special cases
    # However just the regular Parameter() and DateParameter() are the only ones I ever use
    # when building a pipeline.
    filename = luigi.Parameter()

    def run(self):
        # rn is short for row number
        # setting it here will just avoid PEP8 validators from complaining
        # I don't put it in a try catch because if your computer can't set a
        # simple variable to 0 then you have bigger problems
        rn = 0

        # This will attempt to connect to a SQLite database named cutsheet.db that is stored
        # in the DB directory both of which should be located in the current directory
        try:
            connection = sqlite3.connect('DB/cutsheet.db')
            cursor = connection.cursor()
        except Exception as e:
            logger.error("Failed to make connection: " + str(e))
            sys.exit(e)

        # The following will attempt to open up the filename and iterate through it line by line
        # and then insert it into a sqlite database. I am sure that there is a more elegant way to
        # do the following with something like a list comprehension but at the cost of not knowing
        # exactly where each record came from. It will make debugging easier if you run into problems later.
        #
        # The 'with' statement ensures that you close the file.
        try:

            with open('CSV/' + str(self.filename), "rb") as csvfile:
                # The next() function will skip the very first line of the csv file.
                # The other option that you have would be to test values once inside the for loop
                # and test if a value is in the domain of valid values.
                #
                # For example, if the date column should only ever
                # have dates in the format mm/dd/yyyy then a date value of 'banana' is invalid
                # and you should then issue the statement 'continue'.
                next(csvfile)
                reader = csv.reader(csvfile)
                for row in reader:
                    rn += 1
                    destination_data_center = row[0]
                    move_date = row[1]
                    relocation = row[2]
                    move_number = row[3]
                    asset_tag_num = row[4]
                    serial_num = row[5]
                    system_name = row[6]
                    manufacturer = row[7]
                    device_type = row[8]
                    system_model_num = row[9]
                    source_cabinet = row[10]
                    total_rmu = row[11]
                    destination_cabinet = row[12]
                    team = row[13]
                    support_owner = row[14]
                    support_number = row[15]
                    low_u = row[16]
                    high_u = row[17]
                    src_power_src_circuit_num = row[18]
                    src_ps1 = row[19]
                    src_ps2 = row[20]
                    src_ps3 = row[21]
                    src_ps4 = row[22]
                    dest_cab_recepticle_num = row[23]
                    dest_ps1 = row[24]
                    dest_ps2 = row[25]
                    dest_ps3 = row[26]
                    dest_ps4 = row[27]
                    source_ip_address = row[28]
                    current_ip_address = row[29]
                    destination_ip_address = row[30]
                    server_port_id = row[31]
                    switch_slot_port_id = row[32]
                    media = row[33]
                    current_switch_port_num = row[34]
                    new_switch_port_num = row[35]
                    critical_comments_notes = row[36]
                    deinstaller_mover = row[37]
                    deinstaller_mover_date = row[38]
                    data_collector = row[39]
                    data_collector_date = row[40]
                    completed_by = row[41]
                    completed_by_date = row[42]
                    reviewer = row[43]
                    reviewer_date = row[44]
                    # Here is the first time you see a variable defined from another file.
                    # The SQL files are just a file to store plain SQL in and makes for your code
                    # easier to read.
                    insert = INSERT_CUTSHEET_STG
                    insert = insert.format(destination_data_center, move_date, relocation, move_number, asset_tag_num,
                                           serial_num, system_name, manufacturer, device_type, system_model_num,
                                           source_cabinet, total_rmu, destination_cabinet, team, support_owner,
                                           support_number, low_u, high_u, src_power_src_circuit_num, src_ps1,
                                           src_ps2, src_ps3, src_ps4, dest_cab_recepticle_num, dest_ps1, dest_ps2,
                                           dest_ps3, dest_ps4, source_ip_address, current_ip_address,
                                           destination_ip_address, server_port_id, switch_slot_port_id,
                                           media, current_switch_port_num, new_switch_port_num, critical_comments_notes,
                                           deinstaller_mover, deinstaller_mover_date, data_collector,
                                           data_collector_date, completed_by, completed_by_date, reviewer,
                                           reviewer_date, self.filename)
                    cursor.execute(insert)

        # We catch any exceptions that sqlite3 might throw.
        except sqlite3.Error as e:
            logger.error("Error inserting records on row number {} of csv file: {}".format(rn, str(e)))
            sys.exit(e)
        # We catch any exceptions that opening the CSV file and then exit.
        except csv.Error as e:
            logger.error("Error opening CSV file: " + str(e))
            sys.exit(e)
        # We catch any unexpected exceptions and exit.
        except Exception as e:
            logger.info("Error in CSV/sqlite3 try: " + str(e))
            sys.exit(e)

        try:
            # First we commit the records that were previously inserted since everything successfully ran.
            # Then we insert a record into the table task_log to indicate that the prior file
            # has been processed.
            connection.commit()
            insert = INSERT_TASK_LOG.format('ExtractCSV', current_date_nk, str(self.filename), 1)
            cursor.execute(insert)
            connection.commit()
        except Exception as e:
            logger.error("Error while inserting record into task log: " + str(e))

    # Here we have the concept of a luigi Target which does things like check to see if a file
    # exists or in our case checks to see if a record exists in a database.
    #
    # Go to Helpers.SQLiteTarget.py to check out the code there
    def output(self):
        return TaskLogTarget('ExtractCSV', current_date_nk, self.filename)

if __name__ == '__main__':
    luigi.run()
