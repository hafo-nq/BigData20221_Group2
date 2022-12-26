import requests
import json

def string_to_list(string):
    if string == 'N/A':
        return []
    return [s.strip() for s in string.split(',')]

def convert_votes(vote):
    return int(vote.replace(',', ''))

def convert_boxoffice(boxoffice):
    if boxoffice.startswith('$'):
        return int(boxoffice[1:].replace(',', ''))
    return None

def convert_runtime(runtime):
    if runtime.endswith(' min'):
        return int(runtime[:-4])
    return None

def convert_year(year):
    today = 2022
    if len(year) == 4:
        return [year]
    elif len(year) == 5:
        start_year = int(year[:4])
        return [str(y) for y in range(start_year, today + 1)]
    elif len(year) == 9:
        start_year, end_year = [int(y) for y in year.split('–')[:2]]
        return [str(y) for y in range(start_year, end_year + 1)]
    return []


def es_transform(data):
    result = dict()
    for key in data.keys():
        result[key] = None
        if key in ['Genre', 'Director', 'Writer', 'Actors', 'Language', 'Country']:
            try:
                result[key] = string_to_list(data[key])
            except:
                pass
        elif key == 'Year':
            try:
                result[key] = convert_year(data[key])
            except:
                pass
        elif key in ['Metascore', 'imdbRating']:
            try:
                result[key] = float(data[key])
            except:
                pass
        elif key == 'imdbVotes':
            try:
                result[key] = convert_votes(data[key])
            except:
                pass
        elif key == 'Runtime':
            try:
                result[key] = convert_runtime(data[key])
            except:
                pass
        elif key == 'totalSeasons':
            try:
                result[key] = int(data[key])
            except:
                pass
        elif key == 'totalSeasons':
            try:
                result[key] = int(data[key])
            except:
                pass
        elif key == 'BoxOffice':
            try:
                result[key] = convert_boxoffice(data[key])
            except:
                pass
        else:
            try:
                raw = data[key]
                if raw == 'N/A':
                    result[key] = None
                else:
                    result[key] = raw
            except:
                pass
    return result

if __name__=='__main__':

	url = "https://movie-database-alternative.p.rapidapi.com/"

	querystring = {"s":"family","r":"json","page":"1"}

	headers = {
		"X-RapidAPI-Key": "116b330202mshac573bc7e1c9d61p1618c7jsn8f077d38c0d6",
		"X-RapidAPI-Host": "movie-database-alternative.p.rapidapi.com"
	}

	response = requests.request("GET", url, headers=headers, params=querystring)
	# ans = es_transform(response.json())
	print(response.text)