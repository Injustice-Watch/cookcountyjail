# cookcountyjail

This contains data for inmates in the cook county jail obtained from the Sheriff's office as of May 31st with names removed, script used to anonymize names, and script used to determine why each inmate was being held based on case disposition column of data.

Each row in the data represents a charge.

Since case dispositions do not always clearly represent an individual's reason for being held in jail, and each individual may have multiple charges with different dispositions, a ranking system was developed to determine the most likely reason an individual was being held taking into account all of their case dispositions. This can be seen in dirtydata.py.

Accuracy of this data is not guaranteed by the Cook County Sheriff's office.
