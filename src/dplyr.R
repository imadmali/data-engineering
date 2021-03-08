library(dplyr)

### READ DATA

fact_table <- read.csv('./data/fact_table.csv')
dim_table <- read.csv('./data/dim_table.csv')

### RENAME

fact_table %>%
  rename(identifier = id) %>%
  head(3)

### CREATE/DROP COLUMNS

fact_table %>%
  mutate(new_column = "foo") %>%
  head(3)

fact_table %>%
  mutate(new_column = v1+1) %>%
  head(3)

### SELECT

fact_table %>%
  select(id, v0) %>%
  head(3)

### CONDITIONS (CASE STATEMENTS)

fact_table %>%
  mutate(new_column = if_else(v2 == 'Y', 1, 0)) %>%
  head(3)


f <- function(id, v0) {
  if (id == "A" & v0 < 0)
    return('Y')
  else if (id %in% c('D','E') & v0 > 0)
    return('N')
  else
    return(NA)
}

fact_table %>%
  rowwise() %>%
  mutate(new_column = f(id, v0)) %>%
  head(5)

### SORTING

fact_table %>%
  arrange(id, desc(v0)) %>%
  head(3)

### FILTER/WHERE

fact_table %>%
  filter(v0 > 0) %>%
  head(3)

fact_table %>%
  filter(id %in% c('A','B','E')) %>%
  head(3)

fact_table %>%
  filter(!id %in% c('A','B','E')) %>%
  head(3)

### GROUP BY

fact_table %>%
  group_by(id) %>%
  summarize(N = n(),
            v0_sum = sum(v0),
            v1_sum = sum(v1),
            v1_max = max(v1)) %>%
  head(3)

### WINDOW

# lag window
fact_table %>%
  group_by(v2) %>%
  arrange(v0) %>%
  mutate(v0_lag = lag(v0, 1, NA))

# window sum (not supported in dplyr but you can use RcppRoll)
fact_table %>%
  group_by(v2) %>%
  arrange(v2, v1) %>%
  mutate(v1_sum = RcppRoll::roll_sum(v1, 2, align="right", fill=NA),
         v1_sum_left = RcppRoll::roll_sum(v1, 2, align="left", fill=NA))

# cumulative sum
fact_table %>%
  group_by(v2) %>%
  arrange(v2, v1) %>%
  mutate(v1_sum = cumsum(v1))

### JOIN

fact_table %>%
  left_join(dim_table, by=c("id"="identifier"))

### UNION

union_all(fact_table[1:5,],fact_table[6:10,])
