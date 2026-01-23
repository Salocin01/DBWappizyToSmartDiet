from datetime import datetime, time


class MongoRepository:
    @staticmethod
    def build_date_filter(after_date):
        if not after_date:
            return {}

        if hasattr(after_date, "date"):
            date_filter = after_date
        else:
            date_filter = datetime.combine(after_date, time.min)

        return {
            "$or": [
                {"creation_date": {"$gte": date_filter}},
                {"update_date": {"$gte": date_filter}},
            ]
        }

    @staticmethod
    def count_documents(collection, after_date=None, extra_filter=None):
        query = {}
        if extra_filter:
            query.update(extra_filter)
        query.update(MongoRepository.build_date_filter(after_date))
        return collection.count_documents(query)

    @staticmethod
    def find_documents(
        collection,
        after_date=None,
        extra_filter=None,
        projection=None,
        sort_field="creation_date",
        offset=0,
        limit=5000,
    ):
        query = {}
        if extra_filter:
            query.update(extra_filter)
        query.update(MongoRepository.build_date_filter(after_date))
        cursor = collection.find(query, projection).sort(sort_field, 1).skip(offset).limit(limit)
        return list(cursor)
