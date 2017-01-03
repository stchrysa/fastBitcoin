from geoip import open_database
import csv

def findLocation(ip):
	with open_database('data/GeoLite2-City.mmdb') as db:
		match = db.lookup(ip)
		if match is not None:
			return match.location	
		else:	
			print 'no match'
			return (None,None)

with open('input.csv') as infile:
	with open('output.csv', 'w') as csvfile:
		fieldnames = ['ip','latitude','longitude']
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		for line in infile:
			ip=str(line.strip())
			location=findLocation(ip)
			(lat,lon)=location
			if location is not None:
				writer.writerow({'ip':ip,'latitude':str(lat),'longitude':str(lon)})
	
