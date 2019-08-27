import spreadsheets as sp

if __name__ == "__main__":
    sp.preview('federal_does_20190404.xlsx')
    sp.preview('federal_listed_20190404.xlsx')
    sp.preview('national_register_listed_20190404.xlsx')
    sp.preview('national_register_multiple_with_links_2015.xlsx')
    sp.preview('removed_20190404.xlsx', skip=200, count=30)
    sp.preview('nhl_links.xlsx')
    sp.preview('national-historic-landmarks-20181017.xlsx')
    sp.preview('NR_everything_Approved-Accepted-Eligible-Listed-Ineligible-Rejected-Removed-Returned-20190404.xlsx',skip=20)
    sp.preview('NR_everything_Approved-Accepted-Eligible-Listed-Ineligible-Rejected-Removed-Returned-20190404.xlsx',skip=2000)
    sp.preview('NR_everything_Approved-Accepted-Eligible-Listed-Ineligible-Rejected-Removed-Returned-20190404.xlsx',skip=2000)

