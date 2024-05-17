# TP1 - Escalabilidad: Middleware y Coordinación de Procesos

## TP1 - Sistemas Distribuidos I (75.74) 

| Apellido y Nombre    | Padrón | Email                 |
| -------------------- | ------ | --------------------- |
| Alvares Windey Juan  | 95242  | jalvarezw@fi.uba.ar   |
| Montenegro Lucas     | 102412 | lmontenegro@fi.uba.ar |


En el siguiente sistema se implemento un sistema de pipes & filters en el que procesará dos flujos de datos sobre libros para cumplir con 5 tipo de requerimientos.
Los datos fueron obtenidos de https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews

Las consultas que resuelve el sistema son las siguientes:
    
1. Título, autores y editoriales de los libros de categoría "Computers" entre 2000 y 2023 que contengan 'distributed' en su título.
    
1. Autores con títulos publicados en al menos 10 décadas distintas

1. Títulos y autores de libros publicados en los 90' con al menos 500 reseñas.

1. 10 libros con mejor rating promedio entre aquellos publicados en los 90’
con al menos 500 reseñas.

1. Títulos en categoría "Fiction" cuyo sentimiento de reseña promedio esté en
el percentil 90 más alto. 


Para correr las consultas se puede levantar alguno de los archivos de **docker-compose** de la siguiente manera:

Ejemplo para correr la Consulta 3:

`docker-compose -f docker-compose3.yaml up --build`

O bien con el script **run.sh**

Ejemplo para correr la query 5:

`./run.sh q5`

