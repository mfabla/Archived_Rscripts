


library(tidyverse)
library(tidyr)
library(xml2)
library(rvest)
library(xlsx)

setwd("~/MLS Soccer Model")

# Functions ---------------------------------------------------------------

soccer.scrape <- function(url,nodes){
  site <- paste0(url)
  webpage <- read_html(site)
  player.df <- html_nodes(webpage, paste0(nodes)) %>% html_text()
  player.df <- data.frame(matrix(unlist(player.df),ncol = 21, byrow = T), stringsAsFactors = F)
  names(player.df) <- unlist(player.df[1,])
  player.df <- player.df[-1,]
  player.df[] <- lapply(player.df, function(x) type.convert(x))
  player.df$First <- as.character(player.df$First)
  player.df$Last <- as.character(player.df$Last)
  player.df$`UnAst%`<- as.numeric(gsub("%", "",player.df$`UnAst%`))
  player.df$`Touch%` <- as.numeric(gsub("%","", player.df$`Touch%`))
  player.df$date <- Sys.Date()
  player.df %>% View
  old.data <- read.xlsx()
}


soccer.scrape('http://www.americansocceranalysis.com/player-xg-2017/', 'th , td')
