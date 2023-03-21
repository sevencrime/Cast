

from datetime import datetime
t1 = 1659157260
t2 = 1658631600
dt1 = datetime.utcfromtimestamp(t1)
dt2 = datetime.utcfromtimestamp(t2)

diff = (t2-t1).total_seconds()
print(diff)