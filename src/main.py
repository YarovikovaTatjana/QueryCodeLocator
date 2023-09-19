import queue
import configparser
from sys import argv

from src.connection.gitlab_conn import Connector
from src.locator.processors import (FileProducer, SQLJavaProcessorConsumer, SQLLCProcessorConsumer,
                                    SQLDescProcessorConsumer)
from src.locator.writer import FileWriter

"""
Script Entry Point
@:arg GITLAB_URL   gitlab project url
@:arg ACCESS_TOKEN project secret 
"""


def search_sql_in_java():
    # java processing
    javaPipeline = queue.Queue()
    javaFolders = queue.Queue()
    if not config.get("section_JAVA", "JAVA_COUNT_FOLDER"):
        javaFolders.put(config.get("section_JAVA", "JAVA_FOLDER_DEFAULT"))
    else:
        for i in range(1, int(config.get("section_JAVA", "JAVA_COUNT_FOLDER")) + 1):
            javaFolders.put(config.get("section_JAVA", "JAVA_FOLDER_" + str(i)))
    for i in range(1, 3):
        fileProducer = FileProducer(project=project, commit=commit, pipeline=javaPipeline, folder=javaFolders,
                                    regex=config.get("section_JAVA", "JAVA_MASK"))
        fileProducer.start()
    out_java = queue.Queue()
    for i in range(1, 5):
        javaConsumer = SQLJavaProcessorConsumer(project=project, pipeline=javaPipeline, out=out_java,
                                                regex=config.get("section_REGEX", "JAVA_REGEX"), attempts=1)
        javaConsumer.start()
    fileWriter = FileWriter(pipeline=out_java, filePath="out_java.csv", attempts=4)
    fileWriter.start()


def search_sql_in_lc():
    # xml processing (lc)
    lcFolders = queue.Queue()
    lcFolders.put(config.get("section_LC", "LC_FOLDER"))
    lcPipeline = queue.Queue()
    lcFileProducer = FileProducer(project=project, commit=commit, pipeline=lcPipeline, folder=lcFolders,
                                  regex=config.get("section_LC", "LC_MASK"))
    lcFileProducer.start()
    out_lc = queue.Queue()
    for i in range(1, 5):
        lcFileProducer = SQLLCProcessorConsumer(project=project, pipeline=lcPipeline, out=out_lc, attempts=0)
        lcFileProducer.start()
    fileWriter = FileWriter(pipeline=out_lc, filePath="out_lc.csv", attempts=4)
    fileWriter.start()


def search_sql_in_edit():
    # xml processing (edit)
    editFolders = queue.Queue()
    for i in range(1, int(config.get("section_DESC", "DESC_COUNT_FOLDER")) + 1):
        editFolders.put(config.get("section_DESC", "FOLDER_" + str(i)))
    editPipeline = queue.Queue()

    editFileProducer = FileProducer(project=project, commit=commit, pipeline=editFolders, folder=editFolders,
                                    regex=config.get("section_DESC", "EDIT_MASK"))
    editFileProducer.start()
    out_edit = queue.Queue()
    for i in range(1, 5):
        editFileConsumer = SQLDescProcessorConsumer(project=project, pipeline=editPipeline, out=out_edit, attempts=0)
        editFileConsumer.start()
    fileWriter = FileWriter(pipeline=out_edit, filePath="out_edit.csv", attempts=4)
    fileWriter.start()


def search_sql_in_table():
    # xml processing (table-actions, fill, check)
    descFolders = queue.Queue()
    descFolders.put(config.get("section_DESC", "TABLE_FOLDER"))
    for i in range(1, int(config.get("section_DESC", "DESC_COUNT_FOLDER")) + 1):
        descFolders.put(config.get("section_DESC", "FOLDER_" + str(i)))
    descPipeline = queue.Queue()
    descFileProducer = FileProducer(project=project, commit=commit, pipeline=descPipeline, folder=descFolders,
                                    regex=config.get("section_DESC", "TABLE_MASK"))
    descFileProducer.start()
    out_desc = queue.Queue()
    for i in range(1, 5):
        descFileConsumer = SQLDescProcessorConsumer(project=project, pipeline=descPipeline, out=out_desc, attempts=0)
        descFileConsumer.start()
    fileDescWriter = FileWriter(pipeline=out_desc, filePath="out_table.csv", attempts=4)
    fileDescWriter.start()


def search_sql_in_check():
    # xml processing (table-actions, fill, check)
    descFolders = queue.Queue()
    for i in range(1, int(config.get("section_DESC", "DESC_COUNT_FOLDER")) + 1):
        descFolders.put(config.get("section_DESC", "FOLDER_" + str(i)))
    descPipeline = queue.Queue()
    descFileProducer = FileProducer(project=project, commit=commit, pipeline=descPipeline, folder=descFolders,
                                    regex=config.get("section_DESC", "CHECK_MASK"))
    descFileProducer.start()
    out_check = queue.Queue()
    for i in range(1, 5):
        descFileConsumer = SQLDescProcessorConsumer(project=project, pipeline=descPipeline, out=out_check, attempts=0)
        descFileConsumer.start()
    fileDescWriter = FileWriter(pipeline=out_check, filePath="out_check.csv", attempts=4)
    fileDescWriter.start()


def search_sql_in_fill():
    # xml processing (table-actions, fill, check)
    descFolders = queue.Queue()
    for i in range(1, int(config.get("section_DESC", "DESC_COUNT_FOLDER")) + 1):
        descFolders.put(config.get("section_DESC", "FOLDER_" + str(i)))
    descPipeline = queue.Queue()
    descFileProducer = FileProducer(project=project, commit=commit, pipeline=descPipeline, folder=descFolders,
                                    regex=config.get("section_DESC", "FILL_MASK"))
    descFileProducer.start()
    out_fill = queue.Queue()
    for i in range(1, 5):
        descFileConsumer = SQLDescProcessorConsumer(project=project, pipeline=descPipeline, out=out_fill, attempts=0)
        descFileConsumer.start()
    fileDescWriter = FileWriter(pipeline=out_fill, filePath="out_fill.csv", attempts=4)
    fileDescWriter.start()


def search_sql_in_all_desc():
    search_sql_in_edit()
    search_sql_in_table()
    search_sql_in_check()
    search_sql_in_fill()


if __name__ == '__main__':
    if argv.__len__() < 3:
        print("not enough parameters")
        exit(1)
        # get argument's
    script, url, token = argv
    gl = Connector(url, token)
    if not gl.auth():
        exit(1)

    project = gl.getProject()
    if project is None:
        exit(1)

    config = configparser.ConfigParser()  # создаём объекта парсера
    config.read("settings.ini")  # читаем конфиг

    branch = config.get("section_MAIN", "BRANCH")

    # Hash code of the last commit from the master branch
    commit = project.commits.list(ref_name=branch)[0].id

    if config.get("section_MAIN", "JAVA_SEARCH") == 'true':
        search_sql_in_java()
    else:
        print(f'поиск по java файлам пропускается')

    if config.get("section_MAIN", "LC_SEARCH") == 'true':
        search_sql_in_lc()
    else:
        print(f'поиск по lc файлам пропускается')

    if config.get("section_MAIN", "ALL_DESC_SEARCH") == 'true':
        search_sql_in_all_desc()
    else:
        print(f'общий поиск по edit/table-actions/fill/check файлам пропускается')
        if config.get("section_MAIN", "EDIT_SEARCH") == 'true':
            search_sql_in_edit()
        else:
            print(f'поиск по edit файлам пропускается')
        if config.get("section_MAIN", "TABLE_SEARCH") == 'true':
            search_sql_in_table()
        else:
            print(f'поиск по table-actions файлам пропускается')
        if config.get("section_MAIN", "FILL_SEARCH") == 'true':
            search_sql_in_fill()
        else:
            print(f'поиск по fill файлам пропускается')
        if config.get("section_MAIN", "CHECK_SEARCH") == 'true':
            search_sql_in_check()
        else:
            print(f'поиск по check файлам пропускается')
