
"""
    Downloading data from Catastro (Spain) ATOM Service
    author: Javier Belo
    email: javier.belo@outlook.com
    date: 2017-09-07
"""
from feedparser import parse as fp
import urllib.request
import urllib.parse
import os
import csv
import threading

ATOM_DATA = {'addresses': {'url': "http://www.catastro.minhap.es/INSPIRE/Addresses/ES.SDGC.AD.atom.xml",
                               'folder': "/addresses"},
                      'parcels': {'url': "http://www.catastro.minhap.es/INSPIRE/CadastralParcels/ES.SDGC.CP.atom.xml",
                                   'folder': "/parcels"},
                      'buildings': {'url': "http://www.catastro.minhap.es/INSPIRE/buildings/ES.SDGC.BU.atom.xml",
                                     'folder': "/buildings"}}
PATH_ROOT = "/Users/javierbelogarcia/Documents/catastro"


class CSVHandler:

    csv_log = None

    def __init__(self):
        self.rlock = threading.RLock()

    def open_csv(self, path_folder: str) -> None:
        """
        Create new csv file based on the parameters
        If csv already exist then just open it
        :param path_folder:
        :return: None
        :raise: IOError
        """
        if self.csv_log is None:
            self.csv_log = open(path_folder + '/errors.csv', 'wb')

    def write_csv(self, rows: []) -> None:
        """
        Update the csv with the rows passed as argument
        :param rows: array containing the rows to include in the csv
        :return: None
        """
        with self.rlock:
            csv_writer = csv.writer(self.csv_log, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for row in rows:
                csv_writer.writerow(row)


class HardWorker:

    def __init__(self):
        pass

    def folder_setting(self, path_root: str, folder: str) -> str:
        """
        Set up directory, in case directory exist raise an exception. If path is created successfully then
        the string of path directory is returned
        :param path_root: directory root
        :param folder: folder name.
        :return: String directory path of folder
        :raise: OSError in case directory already exist, avoid overwriting
        """
        path_folder = path_root + "/" + folder
        os.makedirs(path_folder, exist_ok=False)
        return path_folder
        pass

    def retrieve(self,atom_url: str, path_folder: str) -> []:
        """
        Iterate parse entries and download zip files with Catastro data of each municipality
        :param atom_url: url of Catastro atom service
        :param path_folder: directory where Catastro data will be downloaded
        :return: array with errors in municipality downloading
        """
        # multidimensional array containing the errors generated during the downloading process
        failures = []
        # amount of entries at first level. In Catastro means provinces amount
        total_entries_lvl_0 = 0
        # atom entries processed at first level. In Catastro means provinces
        entries_lvl_0 = 0
        # atom entries at second level. In Catastro means municipalities
        entries_lvl_1 = 0
        try:
            atom = fp(atom_url)
            entries_first_lvl = atom.entries
            total_entries_lvl_0 = len(entries_first_lvl)
            print('Total provinces: {}'.format(total_entries_lvl_0))
            # iterate the initial entries
            for entry in entries_first_lvl:
                # changing the names
                m_pos = [pos for pos, char in enumerate(entry.title) if char == " "]
                folder_entry = entry.title[m_pos[1]+1:]
                folder_entry = folder_entry.replace(' ','_')
                path_folder_entry = path_folder + '/' + folder_entry
                # raise FileExistsError if the directory already exist. avoid overwriting
                os.mkdir(path_folder_entry)
                # enter in closure
                sub_a = fp(entry.enclosures[0].href)
                entries_second_lvl = sub_a.entries
                for entry_n in entries_second_lvl:
                    file_name = entry_n.title.rsplit(' ', 1)[0]
                    file_name = file_name.replace(' ', '_')
                    path_file = path_folder_entry + '/' + file_name + '.zip'
                    href = entry_n.enclosures[0].href
                    url_enclosure = href.replace('http://', '')
                    url_enclosure = urllib.parse.quote(url_enclosure)
                    try:
                        urllib.request.urlretrieve('http://'+url_enclosure, path_file)
                    # TODO handle exceptions raised by urllib.request...
                    except Exception as e:
                        failures.append([folder_entry, file_name, str(e)])
                    entries_lvl_1 += 1
                entries_lvl_0 += 1
                print('Province: ' + folder_entry + '  Municipalities: ' + str(entries_lvl_1) +
                      '  Provinces downloaded: ' + str(entries_lvl_0) + '/' + str(total_entries_lvl_0))
                entries_lvl_1 = 0
                # if provinces == 5:
                    # break
        except FileExistsError as e:
            print("Exception FileExistsError: " + str(e))
        except Exception as e:
            print("Exception exception: " + str(e))
        return failures


def main():
    print("main function")


if __name__ == '__main__':
    main()

