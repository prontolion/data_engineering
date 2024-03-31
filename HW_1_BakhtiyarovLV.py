
# Обязательная библиотека
import luigi

# Библиотека разархиваторов
import tarfile
import gzip

# Библиотеки для работы с директориями
import os
import shutil
import io

# Библиотека парсинга
import wget

# Библиотека для преобразования таблиц (в этом случае из txt в csv)
import pandas as pd

# --------------------------------------------------------------------------------------


# Класс скачивания архива
class DownloadDataset(luigi.Task):
    dataset_name = luigi.Parameter()

    # То, что мы будем возвращать в нужную директорию
    def output(self):
        return luigi.LocalTarget(f"data/{self.dataset_name}_RAW.tar")

    def run(self):
        # Создаем эту нужную директорию, если она не существует
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        
        # Предполагаем, что ссылка на скачивание захардкодена <--- в любом случае, можно изменить в случае чего
        download_url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/GSE68nnn/{self.dataset_name}/suppl/{self.dataset_name}_RAW.tar"
        wget.download(download_url, self.output().path)


# Класс распаковки архива
class ExtractTarFile(luigi.Task):
    dataset_name = luigi.Parameter()

    # Обращаемся к прошлому процессу (выгрузки)
    def requires(self):
        return DownloadDataset(dataset_name=self.dataset_name)
    
    # Выберем название для места храния "подархивов"
    def output(self):
        return luigi.LocalTarget(f"data/{self.dataset_name}")
    
    def run(self):
        # Создаём такую папку
        os.makedirs(self.output().path, exist_ok=True)
        
        # Помещаем туда архивы
        with tarfile.open(self.input().path, mode="r") as tar:
            tar.extractall(path=self.output().path)


# Класс обработки файлов для их трансформации в текстовые файлы
class ProcessFiles(luigi.Task):
    dataset_name = luigi.Parameter()

    # Обращаемся к прошлому процессу (распаковки)
    def requires(self):
        return ExtractTarFile(dataset_name=self.dataset_name)
    
    # Выберем название для места храния текстовых файлов из архивов
    def output(self):
        return luigi.LocalTarget(f"data/{self.dataset_name}/processed")
    
    # Метод разархивирования:
    # - Не получилось сделать с помощью subprocess
    # - Искал в интернете другие способы, нашёл через gzip + shutil
    def run(self):        
        os.makedirs(self.output().path, exist_ok=True)
        
        # С помощью метода walk и разбивки параметров получаем список файлов в нужной директории
        for root, dir, files in os.walk(self.input().path):
            
            # Для каждого файла, оканчивающегося на gz (архив) применяем алгоритм поиска по имени и двойного раскрытия
            for file in files:
                
                if file.endswith(".gz"):
                    gz_path = os.path.join(root, file)
                    file_name = os.path.splitext(os.path.basename(gz_path))[0]
                    output_file = os.path.join(self.output().path, file_name)
                    
                    # - чтение
                    with gzip.open(gz_path, 'rb') as f_in:
                        # - переписывание и копирование с помощью shutil
                        with open(output_file, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)


class SplitTSVTables(luigi.Task):
    dataset_name = luigi.Parameter()
    
    def requires(self):
        return ProcessFiles(dataset_name=self.dataset_name)
    
    def output(self):
        return luigi.LocalTarget(f"data/{self.dataset_name}/processed")
    
    def run(self):
        processed_path = self.output().path
        
        for file_name in os.listdir(processed_path):
            if file_name.endswith(".txt"):
                file_path = os.path.join(processed_path, file_name)
                self._split_tsv_tables(file_path)
    
    # Алгоритм, который был предложен в условии, 
    # запихнул в метод, чтобы можно было вызывать каждый файл в папке с форматом txt
    def _split_tsv_tables(self, file_path):
        dfs = {}
        
        with open(file_path) as f:
            write_key = None
            fio = io.StringIO()
            
            for l in f.readlines():
                
                if l.startswith('['):
                
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == 'Heading' else 'infer'
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                        
                        fio = io.StringIO()
                    
                    write_key = l.strip('[]\n')
                    
                    continue
                
                if write_key:
                    fio.write(l)
            
            if write_key:
                fio.seek(0)
                dfs[write_key] = pd.read_csv(fio, sep='\t')

        # Сохранение каждой таблицы в отдельный tsv-файл
        for key, df in dfs.items():
            os.makedirs(self.output().path + '/' + key , exist_ok=True)
            
            base_name = os.path.basename(file_path)
            
            # Удаляем расширение .txt и добавляем ключ и расширение .tsv
            output_file_name = f"{key}_{base_name.replace('.txt', '')}.tsv"

            output_file = os.path.join(self.output().path + '/' + key + '/', output_file_name)
            
            # Проверка на доп. условие в задаче
            if key == "Probes":
                df_drop_cols = df.drop(columns=['Definition', 'Ontology_Component', 'Ontology_Process', 'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence'])
                
                output_file_name_reduced = f"{key}_{base_name.replace('.txt', '')}_reduced.tsv"
                output_file_reduced = os.path.join(self.output().path + '/Probes' + '/', output_file_name_reduced)
                
                df_drop_cols.to_csv(output_file_reduced, sep='\t', index=False)

            df.to_csv(output_file, sep='\t', index=False)
    

# Класс очистки после преобразования файлов
# Поскольку с него всё начинается, в него и нужно задавать параметр поиска на сайте и название датасета
# + Папки, которые нужно создать в дальнейшем под распределение по ключам
class Clean(luigi.Task):
    # Датасет
    dataset_name = luigi.Parameter(default='GSE68849')

    # Вызов всех необходимых элементов для парсинга, разархивирования и тд
    def requires(self):
        return SplitTSVTables(dataset_name=self.dataset_name)
    
    # Удаление ненужных архивов
    def run(self):
        for file_name in os.listdir(f"data/{self.dataset_name}"):
            if file_name.endswith(".txt.gz"):
                os.remove(os.path.join(f"data/{self.dataset_name}", file_name))
                
        for file_name in os.listdir(f"data/{self.dataset_name}/processed"):
            if file_name.endswith(".txt"):
                os.remove(os.path.join(f"data/{self.dataset_name}/processed", file_name))
                
        for file_name in os.listdir(f"data"):
            if file_name.endswith(".tar"):
                os.remove(os.path.join(f"data", file_name))
    
    
if __name__ == '__main__':
    luigi.build([Clean()], local_scheduler=True)
    