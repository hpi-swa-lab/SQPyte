# Run: 'python data_gen.py 1000000 data-june-19-2014.csv'
# Produces entries of the form: 'First,Last,age,Country,zip,Frequency'

import sys, os
from random import randint, choice
import string

frequencies = ["Never", "Once", "Seldom", "Often", "Daily", "Weekly", "Monthly", "Yearly"]

countries = ["Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Antigua & Deps", 
             "Argentina", "Armenia", "Australia", "Austria", "Azerbaijan", "Bahamas", 
             "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", 
             "Bhutan", "Bolivia", "Bosnia Herzegovina", "Botswana", "Brazil", "Brunei", 
             "Bulgaria", "Burkina", "Burundi", "Cambodia", "Cameroon", "Canada", 
             "Cape Verde", "Central African Rep", "Chad", "Chile", "China", "Colombia", 
             "Comoros", "Congo", "Congo ", "Costa Rica", "Croatia", "Cuba", "Cyprus", 
             "Czech Republic", "Denmark", "Djibouti", "Dominica", "Dominican Republic", 
             "East Timor", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", 
             "Eritrea", "Estonia", "Ethiopia", "Fiji", "Finland", "France", "Gabon", 
             "Gambia", "Georgia", "Germany", "Ghana", "Greece", "Grenada", "Guatemala", 
             "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Honduras", "Hungary", 
             "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland ", "Israel", 
             "Italy", "Ivory Coast", "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya", 
             "Kiribati", "Korea North", "Korea South", "Kosovo", "Kuwait", "Kyrgyzstan", 
             "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", 
             "Lithuania", "Luxembourg", "Macedonia", "Madagascar", "Malawi", "Malaysia", 
             "Maldives", "Mali", "Malta", "Marshall Islands", "Mauritania", "Mauritius", 
             "Mexico", "Micronesia", "Moldova", "Monaco", "Mongolia", "Montenegro", 
             "Morocco", "Mozambique", "Myanmar", "Namibia", "Nauru", "Nepal", 
             "Netherlands", "New Zealand", "Nicaragua", "Niger", "Nigeria", "Norway", 
             "Oman", "Pakistan", "Palau", "Panama", "Papua New Guinea", "Paraguay", 
             "Peru", "Philippines", "Poland", "Portugal", "Qatar", "Romania", 
             "Russian Federation", "Rwanda", "St Kitts & Nevis", "St Lucia", 
             "Saint Vincent & the Grenadines", "Samoa", "San Marino", "Sao Tome & Principe", 
             "Saudi Arabia", "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", 
             "Slovakia", "Slovenia", "Solomon Islands", "Somalia", "South Africa", 
             "South Sudan", "Spain", "Sri Lanka", "Sudan", "Suriname", "Swaziland", 
             "Sweden", "Switzerland", "Syria", "Taiwan", "Tajikistan", "Tanzania", 
             "Thailand", "Togo", "Tonga", "Trinidad & Tobago", "Tunisia", "Turkey", 
             "Turkmenistan", "Tuvalu", "Uganda", "Ukraine", "United Arab Emirates", 
             "United Kingdom", "United States", "Uruguay", "Uzbekistan", "Vanuatu", 
             "Vatican City", "Venezuela", "Vietnam", "Yemen", "Zambia", "Zimbabwe"]

def get_number_letters_minus_one():
    return randint(1, 9) # inclusive

def generate_name():
    num = get_number_letters_minus_one()
    name = choice(string.ascii_uppercase)
    for i in range(num):
        name += choice(string.ascii_lowercase)
    return name

def generate_age():
    return randint(1, 100)

def generate_country():
    return countries[randint(0, 195)]

def generate_zip():
    return "%05d" % randint(1000, 99999)

def generate_frequency():
    return frequencies[randint(0, 7)]

def generate_entry():
    return "%s,%s,%s,%s,%s,%s\n" % (
        generate_name(),
        generate_name(),
        generate_age(),
        generate_country(),
        generate_zip(),
        generate_frequency())


def run(numentries, filename):
    if os.path.isfile(filename):
        os.remove(filename)
    f = open(filename,'a')
    num = int(numentries)
    for i in range(num):
        f.write(generate_entry())
    f.close()

def entry_point(argv):
    try:
        numentries = argv[1]
        filename = argv[2]
    except IndexError:
        print "You must supply the number of rows and a name for the output file."
        return 1
    
    run(numentries, filename)
    return 0

def target(*args):
    return entry_point
    
if __name__ == "__main__":
    entry_point(sys.argv)
    