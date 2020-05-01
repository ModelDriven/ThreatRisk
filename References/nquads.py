#
# Export a quadstore as nquads to stdout
# python nquads <db> <outputFile>
#   - where <db> is the name of a database and <outputFile is output (defaults to stdout)
#
import os
import io
import sys
import stardog


triplesPerQuery = 1000000

# Change to login for DB
conn_details = {
  'endpoint': 'http://10.109.133.199:7000',
  'username': 'cory',
  'password': 'cory123'
}

kwargs = {
  'limit':triplesPerQuery,
  'offset':0 # Init to zero
}

# Add any graph namespaces holding data here to map value
prefix = {
  'urn:party': 'http://fmbo.fanniemae.com/Parties/',
  'urn:snpcompany' : 'http://fmbo.fanniemae.com/data/snp/company/',
  'urn:snpcompanyrel': 'http://fmbo.fanniemae.com/data/snp/companyrel/',
  'urn:rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
  'urn:owl': 'http://www.w3.org/2002/07/owl#'
}

# Replace reserved characters
def translate(str):
  str = str.replace('\\','\\\\')
  str = str.replace('\n','\\n')
  str = str.replace('\f','\\f')
  str = str.replace('\s','\\s')
  str = str.replace('"','\"')
  str = str.replace("'",'\'')
  return str

# Given a binding and key, return the formatted value
def to_text(binding, key, delim=' '):
  if (not (key in binding)):
    return delim
  val = binding[key]
  if (val['type']=='uri'):
    iri = val['value']
    if iri in prefix:
      iri = prefix[iri]
    return '<'+ iri + '>'+delim
  elif (val['type']=='bnode'):
    return '_:'+val['value']+delim
  else:
    dt = '"'+translate(val['value'])+'"'
    if 'datatype' in val:
      dt += '^^'+val['datatype']
    elif 'xml:lang' in val:
      dt += '@'+val['xml:lang']
    return dt+delim

# Get the name of the DB, argument 1
db = sys.argv[1]
if (len(sys.argv)>2):
  output = open(sys.argv[2], encoding='utf-8', mode="w")
  closeOut = True
else:
  output = sys.stdout
  sys.stderr.write("Output to stdout may cause encoding errors, add file name argument\n")
  closeOut = False


query = 'select ?s ?p ?o ?g where {  {graph ?g {?s ?p ?o }} union {?s ?p ?o }} '

with stardog.Connection(db,  **conn_details) as conn:

  count=triplesPerQuery
  line = 0
  errorCount = 0
  # For each "batch" of triples, query and output
  while count==triplesPerQuery:
    # Do the actual query
    results = conn.select(query,   **kwargs)
    count = 0
    # Output each quad
    for binding in results['results']['bindings']:
      line += 1
      err = False
      quad = to_text(binding, 's')+to_text(binding, 'p')+to_text(binding, 'o')+to_text(binding, 'g', delim='.\n')
      try:
        output.write(quad)
      except:
        sys.stderr.write("\nText Encoding/ stdOut  error triple # {}  \n".format(str(line)))
        sys.stderr.flush()
        err = True
      if err: # Try to detect/fix encoding errors - happens when output is stdout
        errorCount += 1
        try:
          sys.stderr.write("--graph: {} ".format(to_text(binding, 'g')))
        except:
          sys.stderr.write("--error in graph ")
        try:
          sys.stderr.write("--subject: {} ".format(to_text(binding, 's')))
        except:
          sys.stderr.write("--error in subject ")
        try:
          sys.stderr.write("--predicate: {} ".format(to_text(binding, 'p')))
        except:
          sys.stderr.write("--error in predicate ")
        try:
          sys.stderr.write("--object: {} ".format(to_text(binding, 'o')))
        except:
          sys.stderr.write("--error in object ")
        sys.stderr.write("\n")
        try:
          output.write((to_text(binding, 's') + to_text(binding, 'p')+ '"BAD CHARACTER IN VALUE"' + to_text(binding, 'g')) )
        except:
          sys.stderr.write("--Could not write to stdOut without object value, triple ignored\n")

        sys.stderr.flush()

      count += 1
    kwargs['offset'] += count # Set offset for next batch
    output.flush()
    sys.stderr.write("*")
    sys.stderr.flush()

  if (closeOut):
    output.close()
  sys.stderr.write('\nCompleted output of {:d} n-quads, {:d} errors. \n'.format(kwargs['offset'], errorCount))
  sys.stderr.flush()


