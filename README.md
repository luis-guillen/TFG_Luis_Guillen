1) Datos:

Localización de datos

        Asegurarse de que el fichero csv no contiene separadores de miles en los datos y que los decimales son comas
        Hay que dejar los ficheros da datos de partida en la ruta data/ent.
        Usar data/int para guardar los ficheros intermedios
        Usar data/res para guardar los resultados finales

1.- Preparación de datos (1_main_data.py)

        Se parte del fichero "Sabi_Export_agregado bruto paso5.xlsx"
        Se eliminan las filas 2 y 3 (se deja solo el número de la cuenta como nombre de la columna) 
        se ponen todos los nombres de columna en minúsculas
        se cambia year por año
        Este programa se ejecuta 




        Se prepara un csv y se guarda como bam.csv



2) conda environment

Se ha creado un env de conda llamado bam. Una vez creado se puede generar el requirements.txt:

        pip freeze > requirements.txt  (genera un listado de librerias instaladas)
        para instalar los paquetes ejecutar conda install -r requirements.txt

tip: crear env de conda para cada proyecto e instalar las librerías que interesen ; después se genera el requirements.txt



