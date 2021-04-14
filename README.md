# data_validation
This script is used to compare data from csv exports of different sources.
Using a provided column mapping it will perform db style joins using pandas to find missing
records between the sources and also output how much overlap there is in the data not included in a primary key.

Would be even more useful with the implementation of a method to output the delta of aggregate values.
