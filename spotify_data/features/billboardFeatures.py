import os

url_list = list()

with open("BillboardOnlyURL.txt", "r") as bb_url:
    for line in bb_url:
        url_list.append(line[-23:-1])

for i in url_list:
    s_url = '"https://api.spotify.com/v1/audio-features/' + i + '"'
    cmd = 'curl -X GET ' + s_url + ' -H "Accept: application/json" -H "Authorization: Bearer BQDsb2A6knUGcyJ5R2TdCO0B25j5fvFkRkVMBfOawgkTLOF6xVuWm2ijCEkV_53Ep5twoR0bJN5bAlrnMEyhq_09siZsTFB5U6msEh4B_x-rcG5Wx2fwY9-jxFms61iCEPJOLQ"'
    os.system(cmd)

