#!/usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
class clientes_por_pais(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_paso1,
                reducer=self.reducer_paso1),
            MRStep(mapper=self.mapper_paso2)
        ]
    def mapper_paso1(self,_, line):
        linea=line.split(";")
        encontrado=re.search('^[0-9]',linea[0]) #La columna 0 tiene que ser un número.
        #Solo registros
        clave=linea[6] #Cliente
        venta=linea[5] #Importe de la venta
        pais=linea[7]
        if encontrado!=None:
            yield (clave,pais), float(venta)
          
    def reducer_paso1(self, key, values):
        yield key, sum(values)
    
    def mapper_paso2(self,key,values):
        if key[1]=="EIRE":
            yield key, values
    
if __name__ == '__main__':
    clientes_por_pais.run()
