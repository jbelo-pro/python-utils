
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


class SaveCSVHandler:

    csv_log = None

    def __init__(self):
        self.rlock = threading.RLock()

    def open_csv(self, path_root, folder):
        """Create new csv file based on the parameters
        If csv already exist then just open it
        :param path_root:
        :param folder:
        :return:
        :raise: IOError in case
        """
        if self.csv_log is None:
            self.csv_log = open(path_root + folder + '/errors.csv', 'wb')

    def write_csv(self, rows):
        """
        Update the csv with the rows passed as argument
        :param rows: array containing the rows to include in the csv
        :return: None
        """
        with self.rlock:
            csv_writer = csv.writer(self.csv_log, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for row in rows:
                csv_writer.writerow(row)


class HardWorker:
    ATOM_DATA = {'addresses': {'url': "http://www.catastro.minhap.es/INSPIRE/Addresses/ES.SDGC.AD.atom.xml",
                                     'folder': "/addresses"},
                      'parcels': {'url': "http://www.catastro.minhap.es/INSPIRE/CadastralParcels/ES.SDGC.CP.atom.xml",
                                   'folder': "/parcels"},
                      'buildings': {'url': "http://www.catastro.minhap.es/INSPIRE/buildings/ES.SDGC.BU.atom.xml",
                                     'folder': "/buildings"}}
    PATH_ROOT = "/Users/javierbelogarcia/Documents/catastro"

    errors_csv = None
    # TODO csv file shoulb be a class variable

    def __init__(self):
        pass

    def retrieve(self,atom_url, path_root, folder):
        total_provinces = 0
        entries_lvl_0 = 0
        entries_lvl_1 = 0
        # TODO implement multi-processing
        try:
            os.mkdir(path_root+folder)
            errors_csv = open(path_root + folder + '/errors.csv','wb')

            atom = fp(atom_url)
            m_entries = atom.entries
            total_provinces = len(m_entries)
            print('total provincias ' + str(total_provinces))
            folder_name = ''
            # iterate the initial entries
            for m in m_entries:
                # changing the names
                m_pos = [pos for pos, char in enumerate(m.title) if char == " "]
                folder_name = m.title[m_pos[1]+1:]
                folder_name = folder_name.replace(' ','_')
                path_folder = path_root + folder + '/' + folder_name
                try:
                    os.makedirs(path_folder)
                except OSError as e:
                    print("Exception makedirs: " + str(e))
                # enter in closure
                sub_a = fp(m.enclosures[0].href)
                n_entries = sub_a.entries
                for n in n_entries:
                    file_name = n.title.rsplit(' ', 1)[0]
                    file_name = file_name.replace(' ', '_')
                    path_file = path_root + folder + '/' + folder_name + '/' + file_name + '.zip'
                    href = n.enclosures[0].href
                    url_enclosure = href.replace('http://', '')
                    url_enclosure = urllib.parse.quote(url_enclosure)
                    try:
                        urllib.request.urlretrieve('http://'+url_enclosure ,path_file)
                    # TODO handle exceptions raised by urllib.request...
                    except Exception as e:
                        csv_writer = csv.writer(errors_csv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                        csv_writer.writerow([folder_name, href])
                    entries_lvl_1 += 1

                entries_lvl_0 += 1
                print('Province: ' + folder_name + ' Municipalities: ' + str(entries_lvl_1) + ' Provinces downloaded: '
                      + str(entries_lvl_0) + '/' + str(total_provinces))
                entries_lvl_1 = 0
                # if provinces == 5:
                    # break

        except FileExistsError as e:
            print("Exception FileExistsError: " + str(e))
        except Exception as e:
            print("Exception exception " + str(e))


def main():
    print("main function")
    pass


if __name__ == '__main__':
    main()

