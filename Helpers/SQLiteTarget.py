import luigi
import sqlite3
import sys


class TaskLogTarget(luigi.Target):
    def __init__(self, task_name, load_date, file_name):
        self.task_name = task_name
        self.load_date = load_date
        self.file_name = file_name

    def exists(self):
        connection = sqlite3.connect('DB/cutsheet.db')
        cursor = connection.cursor()
        dw = "SELECT IFNULL(COUNT(task_name), 0) c FROM task_log " \
             "WHERE task_name = '{}' AND load_date = {} AND processed = 1 AND file_name = '{}'"
        dw = dw.format(self.task_name, self.load_date, self.file_name)

        try:
            cursor.execute(dw)
            stage_count = cursor.fetchall()[0][0]
            if stage_count == 0 or stage_count is None:
                return False
            else:
                return True
        except Exception as e:
            sys.exit("Exception in the TaskLogTarget exists: " + str(e))