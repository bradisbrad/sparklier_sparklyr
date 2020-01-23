## Check to ensure Java is already installed
system("java -version")

## Install and load sparklyr, tidyverse, and sparklyr.nested
pacman::p_load(tidyverse, sparklyr, sparklyr.nested)

## Install Spark locally (version 2.3 to coincide with book examples)
spark_install("2.3")

## Connecting to our local cluster
sc <- spark_connect(master = "local", version = "2.3")

## Copying data into Spark
cars <- copy_to(dest = sc, df = mtcars)
cars

## View web interface for Spark
# spark_web(sc)

## Easy dplyr verb on Spark dataframe
count(cars)

## You can use normal dplyr verbs and graphing tools without collecting
cars %>% 
  select(hp, mpg) %>% 
  sample_n(100) %>% 
  ggplot(aes(hp, mpg)) +
  geom_point() +
  theme_bw() +
  labs(x = 'Horsepower', y = 'Miles per gallon', title = 'Vehicle Efficiency', subtitle = 'Miles per gallon vs horsepower')

## You can use lm, but it's better to use Spark's actual tools
model <- ml_linear_regression(cars, mpg ~ hp)
model

fake_hp <- copy_to(sc, data.frame(hp = 250 + 10 * 1:10))

model %>% 
  ml_predict(fake_hp) %>% 
  rename(mpg = prediction) %>% 
  mutate(series = 'Prediction') %>% 
  full_join(select(cars, hp, mpg) %>% mutate(series = 'Original')) %>% 
  ggplot(aes(hp, mpg, color = series)) +
  geom_point() +
  theme_bw()

## Reading and writing from Spark
spark_write_csv(cars, 'cars.csv')
cars <- spark_read_csv(sc, "cars.csv")

## Nested data (json, nested dataframes, etc.)
sdf_nest(cars, hp) %>% 
  group_by(cyl) %>% 
  summarize(data = collect_list(data))

## Apply R code into Spark (use sparingly)
cars %>% 
  spark_apply(~round(.x))


## Streaming Data (Evidently it doesn't like playing with R Markdown)
## This stuff is going to end up where Spark is, so in the "sparklier_sparklyr" folder, not Chapter 2 - Getting Started, in this case
dir.create("input")
write.csv(mtcars, "input/cars_1.csv", row.names = F)

stream <- stream_read_csv(sc, "input/") %>% 
  select(mpg, cyl, disp) %>% 
  stream_write_csv("output/")

dir("output", pattern = ".csv")

write.csv(mtcars, "input/cars_2.csv", row.names = F)
dir("output", pattern = ".csv")
stream_stop(stream)

## Logging
spark_log(sc)
spark_log(sc, filter = 'sparklyr')

## Walk it out (Disconnect)
spark_disconnect(sc)
spark_disconnect_all()
