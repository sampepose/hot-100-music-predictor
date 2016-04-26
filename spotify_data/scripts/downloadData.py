import webbrowser

str_first = "https://spotifycharts.com/api/?download=true&limit=200&country=us&recurrence=daily&date="
str_second = "&type=regional"

for i in range(25,30):
    url = str_first + "2015-04-"
    if (i < 10):
        url = url + "0"
        url = url + str(i) + str_second
        webbrowser.open(url, new=0, autoraise=True)

