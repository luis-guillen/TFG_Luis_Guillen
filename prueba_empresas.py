
#############################
#tabla_cuentas.loc[:,"newcol"] = tabla_cuentas.apply(lambda row: lookup_tabla_sectores(row["EMPRESA"],row["CUENTA1"] row["CUENTA2"]),axis=1)
#############################

def lookup_tabla_sectores(year,company,acc1,acc2):
	##Logica mapeo##
	si acc1 ==A and acc2==B:
		ValorSec1 = tabla_sec[tabla_sec['SECTORES']==sec1 && tabla_sec['EMPRESA']==empresa ][VALOR]
		ValorSec2 = tabla_sec[tabla_sec['SECTORES']==sec2 && tabla_sec['EMPRESA']==empresa ][VALOR]
		valor_final = ValorSec1 + Valoresec2
		return valorfinal
	
	si .........:´
		ValorSec1 = tabla_sec[tabla_sec['SECTORES']==sec1 && tabla_sec['EMPRESA']==empresa ][VALOR]
		valor_final = ValorSec1 
		return valorfinal

def creador_tablafinal():
 listavacia = []
 for year in years:
	for e in companies:
 		for i in accounts:
			for j in accounts:
			listavacia.append([y,e,i,j,lookup_tabla_sectores(y,e,i,j)])
		
 resultado = pd.Dataframe(listavacia,col=['empresa','cuenta1','cuenta2'])	
 return resultado
	


