import base64
import configparser
import queue
import re
import threading
import time

import dictionary as dictionary
import uuid
import lxml.etree as ET
from gitlab.v4.objects import Project

# Wrap over queue for logging
FINISH = object()

dictionary = {'docType', 'fieldValue', 'fieldName', 'resultParamName', 'documentType', 'message', 'args.0.value',
              'args.1.value', 'args.2.value', 'tableFilter', 'phoneField', 'documentGuidList', 'keyMapping', 'from',
              'agreementRequired', 'docAgreed', 'countUpdatedRows', 'transformerType', 'tableName', 'docTypeName',
              'mapping', 'IDB_REGISNMBRBO', 'destOrg', 'orgList.0.name', 'CODE_GRBS'}


class FileProducer(threading.Thread):
    """
    SQL scanner for java files, reads files from the queue and parses them for SQL code

    @:argument project     GitLab project API
    @:argument commit      Hash code of the commit to search against
    @:argument pipeline    Queue for subtracting file paths
    @:param folderQueue Queue for find folder path
    """

    def __init__(self, project: Project, commit, pipeline: queue.Queue, folder: queue.Queue, regex):
        super().__init__()
        self.uuid = str(uuid.uuid4())
        self.project = project
        self.pipeline = pipeline
        self.commit = commit
        self.folder = folder
        self.pattern = re.compile(regex, re.IGNORECASE | re.VERBOSE)

    """
    Bypasses the folder and adds java files to the general queue, and folders to the local queue
    @:argument folder relative folder path in GitLab 
    """

    def bypass(self, folder):
        files = self.project.repository_tree(path=folder, ref=self.commit, all=True)
        key = True
        for index, file in enumerate(files):
            path = file['path']
            name = file['name']
            if file['type'] == 'tree':
                self.folder.put(path)
            elif key and self.pattern.search(name):
                self.pipeline.put({'id': file['id'], 'name': name, 'path': path})

    # Bypass all folders
    def run(self):
        print(f'run File Producer-{self.uuid}')
        while not self.folder.empty():
            value = self.folder.get()
            self.bypass(value)

        self.pipeline.put(FINISH)
        print(f'File Producer-{self.uuid} finish')


class SQLJavaProcessorConsumer(threading.Thread):
    """
    SQL scanner for java files, reads files from the queue and parses them for SQL code

    @:argument project  GitLab project API
    @:argument commit   Hash code of the commit to search against
    @:argument pipeline Queue for subtracting file paths
    """

    def __init__(self, project: Project, pipeline: queue.Queue, out: queue.Queue, regex, attempts):
        super().__init__()
        self.uuid = str(uuid.uuid4())
        self.project = project
        self.pipeline = pipeline
        self.out = out
        self.pattern = re.compile(regex, re.IGNORECASE | re.VERBOSE)
        self.attempts = attempts

    """ 
    Implementing stream processing logic 
    """

    def run(self):
        print(f'run java consumer-{self.uuid}')
        file = None
        while file != FINISH:
            time.sleep(1.5)  # sleep
            while not self.pipeline.empty():
                file = self.pipeline.get()
                if file is FINISH:
                    print(f'Принято сообщение о завершении записи\n'
                          f'Читатель java consumer-{self.uuid}, осталось попыток {self.attempts}\n'
                          f'В очереди {self.pipeline.qsize()}')
                    self.pipeline.put(FINISH)  # pushing further
                    if self.attempts > 0:
                        self.attempts -= 1
                    else:
                        break
                try:
                    fileB64 = self.project.repository_blob(file['id'])
                    textFile = base64.b64decode(fileB64['content']).decode()
                    result = self.pattern.search(textFile, re.IGNORECASE)
                    if result:
                        self.out.put({"path": file['path'], "sql": result.groups()})
                except Exception as ex:
                    print(f'{ex}')

        self.out.put('finish')
        print(f'consumer-{self.uuid} завершает свою работу')


class SQLLCProcessorConsumer(threading.Thread):
    def __init__(self, project: Project, pipeline: queue.Queue, out: queue.Queue, attempts):
        super().__init__()
        self.uuid = str(uuid.uuid4())
        self.project = project
        self.pipeline = pipeline
        self.out = out
        self.attempts = attempts

    @staticmethod
    def file_processor(file):
        result = []
        xml_tree = ET.ElementTree(ET.fromstring(file))
        root = xml_tree.getroot()
        # Primary display of tags
        refs = [item for item in root.iter() if item.tag.endswith('call-ref') or item.tag.endswith('call')]
        # Sorting out potentially suitable
        for ref in refs:
            parameters = [item for item in ref.getchildren() if item.tag.endswith("parameters")]
            description = [item for item in ref.getchildren() if item.tag.endswith("description")]
            if parameters.__len__() == 0:
                continue
            for parameter in parameters[0].getchildren():
                name = parameter.attrib['name']
                try:
                    if name in dictionary:
                        continue
                    value = parameter.attrib['value']
                    if value is None:
                        continue

                    if parameter.attrib['name'] == 'sql':
                        result.append({"sql": value, "id": ref.attrib['id'], "description": description[0].text})
                    elif 'fields.' in value:
                        result.append({"sql": value, "id": ref.attrib['id'], "description": description[0].text})
                except KeyError as error:
                    print(f'parameter parsing error {error} with name {name}')
        return result

    def run(self):
        print(f'run lc consumer-{self.uuid}')
        file = None
        while file != FINISH:
            time.sleep(0.5)
            while not self.pipeline.empty():
                file = self.pipeline.get()
                if file is FINISH:
                    print(f'Принято сообщение о завершении записи\n'
                          f'Читатель lc consumer-{self.uuid}, осталось попыток {self.attempts}\n'
                          f'В очереди {self.pipeline.qsize()}')
                    self.pipeline.put(FINISH)  # pushing further
                    if self.attempts > 0:
                        self.attempts -= 1
                    else:
                        break
                fileB64 = self.project.repository_blob(file['id'])
                textFile = base64.b64decode(fileB64['content']).decode()
                path = file['path']
                try:
                    result = self.file_processor(textFile.encode('utf-8'))
                    for r in result:
                        self.out.put(
                            {"path": file['path'], "sql": r['sql'], "id": r['id'], "description": r['description']})
                except Exception as ex:
                    print(f'an error {ex} was encountered while parsing the file {path}')

        self.out.put('finish')
        print(f'lc-consumer-{self.uuid} завершает свою работу')


class SQLDescProcessorConsumer(threading.Thread):
    def __init__(self, project: Project, pipeline: queue.Queue, out: queue.Queue, attempts):
        super().__init__()
        self.uuid = str(uuid.uuid4())
        self.project = project
        self.pipeline = pipeline
        self.out = out
        self.attempts = attempts

    @staticmethod
    def file_check_processor(file):
        result = []
        xml_tree = ET.ElementTree(ET.fromstring(file))
        root = xml_tree.getroot()
        # Primary display of tags
        refs = [item for item in root.iter() if item.tag.endswith('checkGroup')]
        # Sorting out potentially suitable
        for ref in refs:
            checks = [item for item in ref.getchildren() if item.tag.endswith("check")]
            if checks.__len__() == 0:
                continue
            for check in checks:
                parameters = [item for item in check.getchildren() if item.tag.endswith("param")]
                if parameters.__len__() == 0:
                    continue
                for parameter in parameters:
                    name = parameter.attrib['name']
                    try:
                        if name in dictionary:
                            continue
                        value = parameter.text
                        if value is None:
                            continue
                        if (parameter.attrib['name'] == 'sql') or ('fields.' in value):
                            result.append({"sql": value, "id": check.attrib['id'], "desc": check.attrib['desc']})
                    except KeyError as error:
                        print(f'parameter parsing error {error} with name {name}')
        return result

    @staticmethod
    def file_table_processor(file):
        result = []
        xml_tree = ET.ElementTree(ET.fromstring(file))
        root = xml_tree.getroot()
        # Primary display of tags
        refs = [item for item in root.iter() if item.tag.endswith('action')]
        # Sorting out potentially suitable
        for ref in refs:
            parameters = [item for item in ref.getchildren() if item.tag.endswith("proc")]
            descr = [item for item in ref.getchildren() if item.tag.endswith("comment")]
            if parameters.__len__() == 0:
                continue
            for parameter in parameters[0].getchildren():
                name = parameter.attrib['name']
                try:
                    if name in dictionary:
                        continue
                    value = parameter.text
                    if value is None:
                        continue
                    if descr.__len__() > 0:
                        desc = descr[0].text
                    else:
                        desc = "-"
                    if (parameter.attrib['name'] == 'sql') or ('fields.' in value):
                        result.append({"sql": value, "id": ref.attrib['id'], "desc": desc})
                except KeyError as error:
                    print(f'parameter parsing error {error} with name {name}')
        return result

    @staticmethod
    def file_fill_processor(file):
        result = []
        xml_tree = ET.ElementTree(ET.fromstring(file))
        root = xml_tree.getroot()
        # Primary display of tags
        refs = [item for item in root.iter() if item.tag.endswith('procedure')]
        if refs.__len__() == 0:
            return result
        # Sorting out potentially suitable
        for ref in refs:
            for parameter in ref.getchildren():
                name = parameter.attrib['name']
                try:
                    if name in dictionary:
                        continue
                    value = parameter.text
                    if value is None:
                        continue
                    try:
                        desc = ref.attrib["desc"]
                    except KeyError as error:
                        desc = ref.attrib["bean"] + "." + ref.attrib["method"]
                    if (parameter.attrib['name'] == 'sql') or ('fields.' in value):
                        result.append({"sql": value, "id": ref.attrib['field'], "desc": desc})
                except KeyError as error:
                    print(f'parameter parsing error {error} with name {name}')
        return result

    @staticmethod
    def file_edit_processor(file):
        result = []
        xml_tree = ET.ElementTree(ET.fromstring(file))
        root = xml_tree.getroot()
        # Primary display of tags
        refs = [item for item in root.iter() if item.tag.endswith('actions')]
        # Sorting out potentially suitable
        for ref in refs:
            actions = [item for item in ref.getchildren() if item.tag.endswith("action")]
            if actions.__len__() == 0:
                continue
            for action in actions:
                parameters = [item for item in action.getchildren() if item.tag.endswith("proc")]
                descr = [item for item in action.getchildren() if item.tag.endswith("comment")]
                if parameters.__len__() == 0:
                    continue
                for parameter in parameters[0].getchildren():
                    name = parameter.attrib['name']
                    try:
                        if name in dictionary:
                            continue
                        value = parameter.text
                        if value is None:
                            continue
                        if descr.__len__() == 0:
                            desc = "-"
                        else:
                            desc = descr[0].text
                        if (parameter.attrib['name'] == 'sql') or 'fields.' in value:
                            result.append({"sql": value, "id": action.attrib['id'], "desc": desc})
                    except KeyError as error:
                        print(f'parameter parsing error {error} with name {name}')
        return result

    def run(self):
        print(f'run desc consumer-{self.uuid}')
        file = None
        while file != FINISH:
            time.sleep(0.75)
            while not self.pipeline.empty():
                file = self.pipeline.get()
                if file is FINISH:
                    print(f'Принято сообщение о завершении записи\n'
                          f'Читатель desc consumer-{self.uuid}, осталось попыток {self.attempts}\n'
                          f'В очереди {self.pipeline.qsize()}')
                    self.pipeline.put(FINISH)  # pushing further
                    if self.attempts > 0:
                        self.attempts -= 1
                    else:
                        break
                fileB64 = self.project.repository_blob(file['id'])
                textFile = base64.b64decode(fileB64['content']).decode()
                path = file['path']
                section = "default"
                if '.edit' in path:
                    section = "EDIT"
                elif '.table-actions' in path:
                    section = "TABLE"
                elif '.check' in path:
                    section = "CHECK"
                elif '.fill' in path:
                    section = "FILL"
                if section == "default":
                    continue

                try:
                    if section == "EDIT":
                        result = self.file_edit_processor(textFile.encode('utf-8'))
                    elif section == "TABLE":
                        result = self.file_table_processor(textFile.encode('utf-8'))
                    elif section == "FILL":
                        result = self.file_fill_processor(textFile.encode('utf-8'))
                    else:
                        result = self.file_check_processor(textFile.encode('utf-8'))
                    if result.__len__() == 0:
                        continue
                    try:
                        for r in range(len(result)):
                            sql = result[r]['sql']
                            id = result[r]['id']
                            desc = result[r]['desc']
                            try:
                                self.out.put({"path": path, "sql": sql, "id": id, "description": desc})
                            except Exception as e:
                                print(f'an error {e} ошибка добавления в out параметров из файла {path}')
                    except Exception as ex:
                        print(f'an error {ex} ошибка заполнения out')
                except Exception as ex:
                    print(f'an error {ex} was encountered while parsing the file {path}')
        self.out.put('finish')
        print(f'desc-consumer-{self.uuid} завершает свою работу')
