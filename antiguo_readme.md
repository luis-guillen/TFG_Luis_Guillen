1) Datos:

Localización de datos

        Asegurarse de que el fichero csv no contiene separadores de miles en los datos y que los decimales son comas
        Comprobar que no hay cabeceras de los datos en medio del fichero
        Comprobar que no han quedado filas vacías al final de los ficheos csv
        Hay que dejar los ficheros de datos de partida en la ruta data/ent.
        Usar data/int para guardar los ficheros intermedios (datos)
        Usar data/res para guardar los resultados finales (bam)

1.- Preparación de datos (1_main_data.py)

        Se parte del fichero "Sabi_Export_agregado bruto paso5.xlsx"
        Se eliminan las filas 2 y 3 (se deja solo el número de la cuenta como nombre de la columna)
        se ponen todos los nombres de columna en minúsculase
        se cambia year por año
        Este programa se ejecuta

        Se prepara un csv y se guarda como Sabi_Export_?????.csv
        Se procura generar ficheros que no tengan más de 4000 registros

Los ficheros individuales por empresas se guardan en los directorios bam_1, bam_2 etc.
        En bam_1 entrar todos los tur, con, cmi, pru
        En bam_2 entran todos los mayoristas

2) conda environment

Se ha creado un env de conda llamado bam. Una vez creado se puede generar el requirements.txt:

        pip freeze > requirements.txt  (genera un listado de librerias instaladas)
        para instalar los paquetes ejecutar conda install -r requirements.txt

tip: crear env de conda para cada proyecto e instalar las librerías que interesen ; después se genera el requirements.txt

3) En todos los ficheros hay que adaptar la lista de datos (data)

script 1_main_data.py: primera fase de la preparación de datos
        It executes  "data_etl_1(data)"  from prg_1_1_lectura_datos.py which  loads the sabi dat, the mapping files and the IGIC data.
        Saves the pickle files needed later

script 2_main_bam.py: segunda fase de la preparación de datos
         It executes "bam_etl_1.py" from  "prg_1_2_bam.py" which generates the bams and executes:
                                                                bam_generator
                                                                bam_completion
                                                                bam_dictionaries from the lib_bam.py file

script 3_main_prepare_bam: fase de preparación de un fichero por empresa para trabajar con ML
        It executes "bam_etl_3.py" from "prg_1_3_prepare_bam.py"

script 4_main_empresas: preparación de ficheros individuales por empresa