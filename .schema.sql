drop table if exists tweet_fact;
drop table if exists user_fact;

drop table if exists user_dim;
create table [user_dim] (
    user_dim_id int identity(1, 1) primary key,
    user_id int not null unique,
    user_screen_name varchar(20),
    user_name varchar(50),
    user_description varchar(1000),
    user_language varchar(10),
    user_created_at datetime2,
    user_time_zone varchar(10),
    user_location varchar(100)
);

drop table if exists place_dim;
create table [place_dim] (
    place_dim_id int identity(1, 1) primary key,
    place_id int not null unique, 
    place_country_code varchar(20),
    place_country varchar(50),
    place_name varchar(200),
    place_full_name varchar(200),
    place_type varchar(20)
);

drop table if exists tweet_dim;
create table [tweet_dim] (
    tweet_dim_id int identity(1, 1) primary key,
    tweet_id bigint not null unique,
    tweet_text varchar(300),
    tweet_created_at datetime2,
    tweet_source varchar(100),
    tweet_language varchar(20),
    tweet_polarity real,
    tweet_reply_to_status_id int,
    tweet_reply_to_user_id int,
    tweet_reply_to_screen_name varchar(20)    
);

drop table if exists time_dim;
create table [time_dim] (
    time_dim_id int not null identity(1, 1) primary key,
    time_timestamp real not null unique,
    time_seconds int,
    time_minutes int,
    time_hours int,
    time_day int,
    time_month int,
    time_year int
);


create table tweet_fact (
    tweet_fact_id int identity(1, 1) primary key,
    tweet_dim_id int not null ,
    user_dim_id int not null,
    time_dim_id int not null,
    place_dim_id int not null,
    retweet_count int,
    favorite_count int,
    foreign key (tweet_dim_id) references tweet_dim(tweet_dim_id),
    foreign key (user_dim_id) references user_dim(user_dim_id),
    foreign key (time_dim_id) references time_dim(time_dim_id),
    foreign key (place_dim_id) references place_dim(place_dim_id)
)

create table user_fact (
    user_fact_id int identity(1, 1) primary key,
    user_dim_id int not null,
    time_dim_id int not null,
    friends_count int,
    followers_count int,
    listed_count int,
    statuses_count int,
    favorites_count int,
    foreign key (user_dim_id) references user_dim(user_dim_id),
    foreign key (time_dim_id) references time_dim(time_dim_id)
)


SELECT * from user_dim;
