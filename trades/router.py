class ScripCacheRouter:
    def db_for_read(self, model, **hints):
        if model._meta.model_name == 'activemarketdata':
            return 'scrip_cache'
        return None

    def db_for_write(self, model, **hints):
        if model._meta.model_name == 'activemarketdata':
            return 'scrip_cache'
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if model_name == 'activemarketdata':
            return db == 'scrip_cache'
        return None
