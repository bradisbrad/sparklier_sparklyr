## Full Example from Chapter 3
if(!require(pacman)) install.packages('pacman'); library(pacman)
pacman::p_load(tidyverse, sparklyr, corrr, dbplot)

# Connect to Spark
sc <- spark_connect(master = "local", version = "2.3")

# Copy mtcars to Spark
cars <- copy_to(sc, mtcars)

# dplyr just converts to SQL and sends to Spark
cars %>% 
  summarize_all(mean) %>% 
  show_query()

# dplyr verbs just work, though
cars %>% 
  mutate(transmission = ifelse(am == 0, "automatic", "manual")) %>% 
  group_by(transmission) %>% 
  summarize_all(mean)

# If dplyr runs into an unknown function, it passes the function as it stands to Hive SQL
# This explains the collect_list() issue from Chapter 2 also

## R version doesn't work on Spark table
cars %>% 
  summarize(mpg_percentile = quantile(mpg, probs = 0.25))

## Hive version does
cars %>% 
  summarize(mpg_percentile = percentile(mpg, 0.25))

# We can include multiple values with Hive's array function, which returns a list
cars %>% 
  summarize(mpg_percentile = percentile(mpg, array(0.25, 0.5, 0.75)))

# But to actually view these, we'll use explode within a mutate
cars %>% 
  summarize(mpg_percentile = percentile(mpg, array(0.25, 0.5, 0.75))) %>% 
  mutate(mpg_percentile = explode(mpg_percentile))

# corrr includes a backend for Spark, so corrr functions work the same as Spark functions
# i.e. corrr::correlate() == sparklyr::ml_corr()
correlate(cars, use = "pairwise.complete.obs", method = "pearson")
ml_corr(cars)

# And of course, it's very easy to pipe into other stuff
correlate(cars, use = "pairwise.complete.obs", method = "pearson") %>% 
  rearrange() %>% 
  shave() %>% 
  rplot()


# There are also tools in dbplot that give an all in one step instead of transforming on Spark, collecting, and then plotting in R
cars %>% 
  dbplot_histogram(mpg, binwidth = 3) +
  theme_bw() +
  labs(title = "MPG Distribution",
       subtitle = "Histogram over miles per gallon")

# Otherwise, you'd have to create splits of mpg on Spark, count on Spark, collect into R and the plot in R

# A problem with this "push compute, collect results" methodology is scatterplots, which require pulling the entire set
mtcars %>% 
  ggplot(aes(mpg, wt)) +
  geom_point() +
  theme_bw()

# The best alternative to that in Spark is probably a raster plot, which returns a grid of xy positions and plots aggregations
cars %>% 
  dbplot_raster(mpg, wt, resolution = 16) +
  theme_bw()
  
# Modeling works the same as it would otherwise, but, of course, use the different verbs
cars %>% 
  ml_linear_regression(mpg ~ .) %>% 
  summary()

# Easy to switch because of the formula format too
cars %>% 
  ml_linear_regression(mpg ~ hp + cyl) %>% 
  summary()

# Even different models too, although glm vs. lm isn't the strongest display of this
cars %>% 
  ml_generalized_linear_regression(mpg ~ hp + cyl) %>% 
  summary()


# Caching is a good idea if you have to run transformations before running the model
cached_cars <- cars %>% 
  mutate(cyl = paste0("cyl_", cyl)) %>% 
  compute("cached_cars")

cached_cars %>% 
  ml_linear_regression(mpg ~ .) %>% 
  summary()


# R Markdown evidently works with Spark fine, it was just finnicky for me yesterday

# Goodnight everybody!
spark_disconnect(sc)
