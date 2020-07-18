import rejustify

# setup
setCurl()
register(token='YOUR_TOKEN', email='YOUR_EMAIL')

# for testing
df = pd.DataFrame()
df['country'] = ['Italy'] * 4
df['date'] = pd.date_range('2020-06-01', periods=4).strftime("%Y %m %d")
df['covid cases'] = ""

st = rejustify.analyze(df)
print(st)
st = rejustify.adjust(st, id=1, items={'feature': 'month'})
print(st)
