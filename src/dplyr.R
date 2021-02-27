library(dplyr)

N <- 10
num_ids <- 5
id_values <- LETTERS[1:num_ids]
fact_table <- data.frame(id = sample(id_values, size=N, replace=TRUE),
                         v0 = rnorm(N, 0, 1),
                         v1 = rbinom(N, 10, 0.5),
                         v3 = sample(c("Y","N"), size=N, replace=TRUE))

dim_table <- data.frame(identifier = id_values,
                        info = paste0("meta_", id_values),
                        region = sample(1:3, size=num_ids, replace=TRUE))

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
  mutate(new_column = if_else(v3 == 'Y', 1, 0)) %>%
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
            sum_v0 = sum(v0),
            max_v1 = max(v1)) %>%
  head(3)

### WINDOW

# lag window
fact_table %>%
  group_by(v3) %>%
  arrange(v0) %>%
  mutate(v0_lag = lag(v0, 1, NA))

# window sum (not supported in dplyr but you can use RcppRoll)
fact_table %>%
  group_by(v3) %>%
  arrange(v3, v1) %>%
  mutate(v1_sum = RcppRoll::roll_sum(v1, 2, align="right", fill=NA),
         v1_sum_left = RcppRoll::roll_sum(v1, 2, align="left", fill=NA))

# cumulative sum
fact_table %>%
  group_by(v3) %>%
  arrange(v3, v1) %>%
  mutate(v1_sum = cumsum(v1))

### JOIN

fact_table %>%
  left_join(dim_table, by=c("id"="identifier"))

### UNION

union_all(fact_table[1:5,],fact_table[6:10,])
