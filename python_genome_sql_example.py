#!/usr/bin/env python
"""Simple script to check nearby genes using UCSC's Public Genome Browser's MySQL database
   given the chromosome, txStart, and txEnd starting reference points

   Based on code from: http://genomewiki.ucsc.edu/index.php/Finding_nearby_genes
"""

__author__ = "Matthew Carras"
__license__ = "Public Domain"
__version__ = "0.1"

import pymysql
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
def print_sql_results( cursor, sSQL, tPlaceholders, lColHeaders ):
	try:
		# Use placeholders here instead of string concat
		cursor.execute(sSQL, tPlaceholders )
		result = cursor.fetchall()

		if not result: 
			print("ERROR: Oops, we got an empty result. Are all your inputs correct?")
		else:
			# BeautifulTable setup
			table = BeautifulTable()
			table.column_headers = lColHeaders
			
			# Append our results to our BeautifulTable and then print
			for row in result:
				table.append_row( row )
			
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

		print_sql_results( cursor, sql, (chrom,txStart,limit), ["chrom", "txStart", "txEnd", "strand", "name", "geneSymbol"] )
		
		# Closest <limit> downstream transcripts
		print( "\n\nclosest %s downstream transcripts from %s:%s-%s in %s for refGene set\n" % (limit,chrom,txStart,txEnd,db) +
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
			   
		print_sql_results( cursor, sql, (chrom,txStart,limit), ["chrom", "txStart", "txEnd", "strand", "name", "geneSymbol"] )
finally:
	connection.close()


