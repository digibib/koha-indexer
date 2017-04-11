FROM alpine
ADD koha-indexer /koha-indexer
CMD ["/koha-indexer"]
