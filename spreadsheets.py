import xlrd
import datetime
from xlrd import open_workbook
import psycopg2

def preview(filename, skip=10, count=10):
    print (f'File: {filename}: showing {count} rows, skipping first {skip}')
    wb = open_workbook(filename)
    for s in wb.sheets():
        print ('Sheet:', s.name, s.nrows, 'rows')
        col_names = s.row(0)
        print (col_names)
        for row in range(skip, skip+count):
            col_value = []
            for col in range(s.ncols):
                value  = (s.cell(row,col).value)
                try:
                    value = str(int(value))
                except:
                    pass
                col_value.append(value)
            print(col_value)
        print ('\n\n\n')

def cleaned_name(namestring):
    result = namestring
    
    # ampersands changed to " and "
    result = result.replace('&amp;', ' and ')
    result = result.replace('&', ' and ')
    # commas changed to ", "
    result = result.replace(',', ', ')
    # semicolons changed to "; "
    result = result.replace(';', '; ')
    # periods changed to ". "
    result = result.replace('.', '. ')
    # all multiple space runs changed to one space
    result = result.replace('  ', ' ')
    result = result.replace('  ', ' ')
    result = result.replace('  ', ' ')
    result = result.replace('  ', ' ')
    result = result.replace('  ', ' ')
    # ". ," changed to ".,"
    result = result.replace('. ,', '.,')
    # ". ;" changed to ".;"
    result = result.replace('. ;', '.;')
    # Et al. -> et al.
    # Et. al. -> et al.
    # et. al. -> et al.
    result = result.replace('Et al.', 'et al.')
    result = result.replace('Et. al.', 'et al.')
    result = result.replace('et. al.', 'et al.')
    # U. S. A. to USA
    result = result.replace('U. S. A.', 'U.S.A.')
    # U. S. to U.S.
    result = result.replace('U. S. ', 'U.S. ')
    # trim whitespace
    result = result.strip()
    return result

def load_listed_properties_20171205():
    wb = open_workbook('national-register-listed-properties-20171205.xlsx')
    s = wb.sheets()[0]
    print ('Sheet:', s.name, s.nrows)
    with psycopg2.connect("dbname=nrhp user=xyzzy") as conn:
        curs = conn.cursor()
        for row in range( 1, s.nrows):
            refnum = s.cell(row, 0).value
            prefix = s.cell(row, 1).value
            resname = s.cell(row, 2).value
            other_names_list = cleaned_name(s.cell(row, 3).value)
            multname = s.cell(row, 4).value
            listing_date_number = s.cell(row, 5).value
            nhl_date_number = s.cell(row, 6).value
            federal_agency = s.cell(row, 7).value
            national_park = s.cell(row, 8).value
            city = s.cell(row, 9).value
            county = s.cell(row, 10).value
            state = s.cell(row, 11).value
            address = s.cell(row, 12).value
            sig_person_list = cleaned_name(s.cell(row, 14).value)
            architect_list = cleaned_name(s.cell(row, 15).value)

            if listing_date_number:
                year, month, day, hour, minute, second = xlrd.xldate_as_tuple(listing_date_number, wb.datemode)
                listing_date = datetime.datetime(year, month, day,  12, 0, 0)
            else:
                listing_date = None
            if nhl_date_number:
                year, month, day, hour, minute, second = xlrd.xldate_as_tuple(nhl_date_number, wb.datemode)
                nhl_date = datetime.datetime(year, month, day, 12, 0, 0)
            else:
                nhl_date = None

    #['Ref#', 'Prefix', 'Historic Name', 'Other Name(s)', 'Multiple Name', 'Listing Date', 'NHL Date', 'Federal Agency', 'National Park', 'City', 'County', 'State', 'Address', 'Restricted', 'Significant Person', 'Architect', 'Request Type', 'Status', 'Secondary Code']
            print(refnum, prefix, resname, multname, federal_agency, national_park, city, county, state, address)
            print("           ", listing_date_number, listing_date, nhl_date_number, nhl_date)
            print("    other: ", other_names_list)
            print("      sig: ", sig_person_list)
            print("     arch: ", architect_list)

            propquery = "INSERT INTO properties  (refnum, resname, address, state, county, city, certdate, multname, parkname) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT properties_pkey DO NOTHING"
            propvalues = (refnum, resname, address, state, county, city, listing_date, multname, national_park)
            curs.execute(propquery, propvalues)
            for other_name in other_names_list.split('; '):
                print(other_name)
                if len(other_name) > 0:
                    anquery = "INSERT INTO altnames (altname, refnum) VALUES (%s, %s) ON CONFLICT ON CONSTRAINT altnames_altname_refnum_key DO NOTHING"
                    anvalues = (other_name, refnum)
                    curs.execute(anquery, anvalues)
            for sigperson in sig_person_list.split('; '):
                print(sigperson)
                if len(sigperson) > 0:
                    snquery = "INSERT INTO signames (signame, refnum) VALUES (%s, %s) ON CONFLICT ON CONSTRAINT signames_signame_refnum_key DO NOTHING"
                    snvalues = (sigperson, refnum)
                    curs.execute(snquery, snvalues)
            for architect in architect_list.split('; '):
                print(architect)
                if len(architect) > 0:
                    arcquery = "INSERT INTO architects (architect, refnum) VALUES (%s, %s) ON CONFLICT ON CONSTRAINT architects_architect_refnum_key DO NOTHING"
                    arcvalues = (architect, refnum)
                    curs.execute(arcquery, arcvalues)
        curs.close()
        conn.commit()
    print ('\n\n\n')

def load_listed_properties_20190404(dryrun=1):
    wb = open_workbook('national_register_listed_20190404.xlsx')
    s = wb.sheets()[0]
    print ('Sheet:', s.name, s.nrows, 'rows')
    with psycopg2.connect("dbname=nrhp user=xyzzy") as conn:
        curs = conn.cursor()
        for row in range( 1, s.nrows):
            refnum = s.cell(row, 0).value
            resname = s.cell(row, 1).value
            # status: 2
            # restricted address?: 3
            multname = s.cell(row, 4).value.strip()
            state = s.cell(row, 5).value.capitalize()
            county = s.cell(row, 6).value
            city = s.cell(row, 7).value
            address = s.cell(row, 8).value
            listing_date_number = s.cell(row, 9).value
            nhl_date_number = s.cell(row, 10).value
            architect_list = cleaned_name(s.cell(row, 11).value)
            federal_agency = s.cell(row, 12).value
            other_names_list = cleaned_name(s.cell(row, 13).value)
            national_park = s.cell(row, 14).value
            sig_person_list = cleaned_name(s.cell(row, 15).value)

            if listing_date_number:
                year, month, day, hour, minute, second = xlrd.xldate_as_tuple(listing_date_number, wb.datemode)
                listing_date = datetime.datetime(year, month, day,  12, 0, 0)
            else:
                listing_date = None
            if nhl_date_number:
                year, month, day, hour, minute, second = xlrd.xldate_as_tuple(nhl_date_number, wb.datemode)
                nhl_date = datetime.datetime(year, month, day, 12, 0, 0)
            else:
                nhl_date = None

            #[text:'Ref#', text:'Property Name', text:'Status', text:'Restricted Address', text:'Name of Multiple Property Listing', text:'State', text:'County', text:'City ', text:'Street & Number', text:'Listed Date', text:'NHL Designated Date', text:'Architects/Builders', text:'Federal Agencies', text:'Other Names', text:'Park Name', text:
            if dryrun:
                print(refnum, resname, multname, federal_agency, national_park, city, county, state, address)
                print("           ", listing_date_number, listing_date, nhl_date_number, nhl_date)
                print("    other: ", other_names_list)
                print("      sig: ", sig_person_list)
                print("     arch: ", architect_list)

            propquery = "INSERT INTO properties  (refnum, resname, address, state, county, city, certdate, multname, parkname) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT properties_pkey DO NOTHING"
            propvalues = (refnum, resname, address, state, county, city, listing_date, multname, national_park)
            if dryrun:
                print(curs.mogrify(propquery, propvalues))
            else:
                curs.execute(propquery, propvalues)
            for other_name in other_names_list.split('; '):
                if dryrun:
                    print(other_name)
                if len(other_name) > 0:
                    anquery = "INSERT INTO altnames (altname, refnum) VALUES (%s, %s) ON CONFLICT ON CONSTRAINT altnames_altname_refnum_key DO NOTHING"
                    anvalues = (other_name, refnum)
                    if dryrun:
                        print(curs.mogrify(anquery, anvalues))
                    else:
                        curs.execute(anquery, anvalues)
            for sigperson in sig_person_list.split('; '):
                if dryrun:
                    print(sigperson)
                if len(sigperson) > 0:
                    snquery = "INSERT INTO signames (signame, refnum) VALUES (%s, %s) ON CONFLICT ON CONSTRAINT signames_signame_refnum_key DO NOTHING"
                    snvalues = (sigperson, refnum)
                    if dryrun:
                        print(curs.mogrify(snquery, snvalues))
                    else:
                        curs.execute(snquery, snvalues)
            for architect in architect_list.split('; '):
                if dryrun:
                    print(architect)
                if len(architect) > 0:
                    arcquery = "INSERT INTO architects (architect, refnum) VALUES (%s, %s) ON CONFLICT ON CONSTRAINT architects_architect_refnum_key DO NOTHING"
                    arcvalues = (architect, refnum)
                    if dryrun:
                        print(curs.mogrify(arcquery, arcvalues))
                    else:
                        curs.execute(arcquery, arcvalues)
        curs.close()
        conn.commit()
    print ('\n\n\n')
    
def load_properties_removed_20190404(dryrun=1):
    wb = open_workbook('removed_20190404.xlsx')
    s = wb.sheets()[0]
    print ('Sheet:', s.name, s.nrows, 'rows')
    with psycopg2.connect("dbname=nrhp user=xyzzy") as conn:
        curs = conn.cursor()
        for row in range( 1, s.nrows):
            refnum = s.cell(row, 0).value
            resname = s.cell(row, 1).value
            status = s.cell(row, 2).value
            status_date_number = s.cell(row, 3).value

            if status_date_number:
                year, month, day, hour, minute, second = xlrd.xldate_as_tuple(status_date_number, wb.datemode)
                status_date = datetime.datetime(year, month, day,  12, 0, 0)
            else:
                status_date = None

            removedquery = "INSERT INTO removed  (refnum, resname, status, statusdate) VALUES (%s, %s, %s, %s) ON CONFLICT ON CONSTRAINT removed_pkey DO NOTHING"
            removedvalues = (refnum, resname, status, status_date)
            if dryrun:
                print(curs.mogrify(removedquery, removedvalues))
            else:
                curs.execute(removedquery, removedvalues)
        curs.close()
        conn.commit()
            
