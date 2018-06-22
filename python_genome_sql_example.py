#!/usr/bin/env python
"""Simple script to check nearby genes using UCSC's Public Genome Browser's MySQL database
   given the chromosome, txStart, and txEnd starting reference points

   Based on code from: http://genomewiki.ucsc.edu/index.php/Finding_nearby_genes
"""

__author__ = "Matthew Carras"
__license__ = "Public Domain"
__version__ = "0.2"

import pymysql.cursors
from beautifultable import BeautifulTable


# CONFIGURE BELOW

# Change MySQL host, server, etc. here
# For our example we're using UCSC's Public Genome Browser
# We'll be using the hg19 database by default, which is an older database used by the original code
# Server specifications: http://genome.ucsc.edu/goldenPath/help/mysql.html
host='genome-mysql.soe.ucsc.edu' # US West Coast server
user='genomep'
password='password'
db='hg19'

# Chromosome reference point (string, default: 'chr1')
# Range: chr1-chr22, chrX, chrY, chrM, etc.
# See http://genome.ucsc.edu/cgi-bin/hgTables?hgsid=677305531_wYyEDx3ujBsBqwUaACQ6ou1ks9Fa&hgta_database=hg19&hgta_histoTable=knownGene&hgta_doValueHistogram=chrom
chrom='chr1'
# Start for reference point (unsigned int, default: 991973)
# Range: 0 to 248912689
txStart=991973
# End for reference point (unsigned int, default: 991973)
# Range: 0 to 248912689
txEnd=991973
# Number of nearby transcripts to output (unsigned int, default: 10)
# WARNING: UCSC may not allow this limit to be very high if done externally without proper credentials
limit=10

# END CONFIG

# Function: print_sql_results
# Quick function to print the results of our SQL queries using BeautifulTable
# cursor - The pymysql cursor object
# sSQL - MySQL statement string
# tPlaceholders - Tuple of placeholder strings in the SQL statement, ex: (chrom,txStart,limit)
# lColHeaders - List of column headers for BeautifulTable
# ttAddlComputedCols - (Optional) Additional computed columns in the format of: 
#					   	String name of column, Value #1, Operation (String: '+','-','*','/', etc.), Value #2
#					   Value #1 and Value #2 may be a number or a string, where the string would be the row # from the SQL SELECT statement
#					   You may give as many additional columns this way as you like, as tuples of tuples.
#					   ex: (("%s-txEnd" % txStart, txStart, '-', "2")) would be (given_number - value_of_row2)
#						   if txStart=991973 and one SQL row result=991499, then computed value shown is 991973-991499=474
def print_sql_results( cursor, sSQL, tPlaceholders, lColHeaders, ttAddlComputedCols=None ):

	# Sanity checks for given parameters
	if not isinstance(cursor, pymysql.cursors.Cursor):
		print("ERROR: Invalid parameter #1 to print_sql_results() - must be pymysql.cursors.Cursor object")
		return -1
	if not isinstance(sSQL, str):
		print("ERROR: Invalid parameter #2 to print_sql_results() - must be string")
		return -1
	if not type(tPlaceholders) is tuple:
		print("ERROR: Invalid parameter #3 to print_sql_results() - must be tuple")
		return -1
	if not type(lColHeaders) is list:
		print("ERROR: Invalid parameter #4 to print_sql_results() - must be list")
		return -1
	if ttAddlComputedCols is not None:
		if not type(ttAddlComputedCols) is list or not type(ttAddlComputedCols[0]) is list:
			print("ERROR: Invalid parameter #5 to print_sql_results() - must be list of lists")
			return -1
		if len(ttAddlComputedCols[0]) != 4:
			print("ERROR: Invalid parameter #5 to print_sql_results() - each list must be of length 4")
			return -1
		
	try:	
		# Run the SQL statement (hardcoded to be SELECT) and fetch all rows
		cursor.execute(sSQL, tPlaceholders )
		result = cursor.fetchall()

		if not result: 
			print("ERROR: Oops, we got an empty result. Are all your inputs correct?")
		else:		
		
			# Save original column length
			colLengthOrig = len(lColHeaders)
			
			# Add the additional columns, if given
			if ttAddlComputedCols is not None:
				for compentry in ttAddlComputedCols:
					lColHeaders.append( compentry[0] )
						
			# BeautifulTable setup
			table = BeautifulTable()
			table.column_headers = lColHeaders
			
			# Append our SQL results to our BeautifulTable and populate the additional columns
			length_mismatch_warning_given=False
			for row in result:
				# Double-check to see if we have more rows from SQL statement than given column headers
				# (yes, this will print out for each row)
				
				rowLength = len(row)
				if not length_mismatch_warning_given and rowLength > colLengthOrig:
					print('ERROR: print_sql_results() given only %s column headers, but SQL statement resulted in %s columns' %(colLengthOrig,rowLength) )
					length_mismatch_warning_given = True
					
				# Check to see if we have additional computed columns
				if ttAddlComputedCols is not None:
					row = list(row) # convert to list so we can append
					for compentry in ttAddlComputedCols:
						# If string, then it's a row #, otherwise just use given value, using sanity checking first
						if isinstance(compentry[1], str):
							if not compentry[1].isdigit():
								print('ERROR: Computed row element #1, %s, is invalid -- string must be a valid row number' % compentry[1])
							else:
								index = int(compentry[1])
								if index >= rowLength or index < 0:
									print('ERROR: Computed row element #1, %s, is out of range for given SQL statement' % index)
								else:
									x = row[ index ]
						else:
							x = compentry[1]
							
						if isinstance(compentry[3], str):
							if not compentry[3].isdigit():
								print('ERROR: Computed row element #3, %s, is invalid -- string must be a valid row number' % compentry[3])
							else:
								index = int(compentry[3])
								if index >= rowLength or index < 0:
									print('ERROR: Computed row element #3, %s, is out of range for given SQL statement' % index)
								else:
									y = row[ index ]
						else:
							y = compentry[3]
							
						try:
							value = eval('%s %s %s' % (x,compentry[2],y))
							row.append(value) # append our computed value to the row (now a list) returned by the SQL statement
						except:
							print('ERROR: Invalid math given for computed row, %s %s %s' % (x,compentry[2],y))
				
				# Append row (list or tuple) to our BeautifulTable table
				table.append_row( row )
			
			# Print out our BeautifulTable
			print(table)
			
	except pymysql.MySQLError as e:
		print('ERROR: Got error {!r}, errno is {}'.format(e, e.args[0]))
		
	return
# end print_sql_results()

# Begin connection and cursor blocks
connection = pymysql.connect(
    host=host,
    user=user,
    password=password,
    db=db,
)

try:
	with connection.cursor() as cursor:
		
		# Again, this code is a partial conversion of a bash script from http://genomewiki.ucsc.edu/index.php/Finding_nearby_genes
		# Closest <limit> upstream transcripts
		print( "closest %s upstream transcripts from %s:%s-%s in %s for refGene set\n" % (limit,chrom,txStart,txEnd,db) +
			   "last column is distance from reference point to transcript, %s - txEnd\n" % txStart  +
			   "Note: for reverse - strand items, txEnd is the 5' end, the transcription start site"  )
		sql = """SELECT e.chrom,
					   e.txStart,
					   e.txEnd,
					   e.strand,
					   e.name,
					   j.geneSymbol
			   FROM  refGene e,
				     kgXref j
			   WHERE e.name = j.refseq AND e.chrom=%s AND e.txEnd < %s
			   ORDER BY e.txEnd DESC limit %s"""

		#def print_sql_results( cursor, sSQL, tPlaceholders, lColHeaders, ttAddlComputedCols=None )
		print_sql_results( cursor, sql, (chrom,txStart,limit), ["chrom", "txStart", "txEnd", "strand", "name", "geneSymbol"], [["%s-txEnd" % txStart, txStart, '-', '2']] )
		
		#Closest <limit> downstream transcripts
		print( "\n\nclosest %s downstream transcripts from %s:%s-%s in %s for refGene set\n" % (limit,chrom,txStart,txEnd,db) +
			   "last column is distance from reference point to transcript, %s - txStart\n" % txEnd  +
			   "Note: for reverse - strand items, txStart is the 3' end, NOT the transcription start site"  )
		sql = """SELECT e.chrom,
					   e.txStart,
					   e.txEnd,
					   e.strand,
					   e.name,
					   j.geneSymbol
			   FROM  refGene e,
				     kgXref j
			   WHERE e.name = j.refseq AND e.chrom=%s AND e.txStart > %s
			   ORDER BY e.txStart ASC limit %s"""
			   
		print_sql_results( cursor, sql, (chrom,txStart,limit), ["chrom", "txStart", "txEnd", "strand", "name", "geneSymbol"], [["%s-txEnd" % txEnd, txEnd, '-', '1']] )
finally:
	connection.close()


