from django.db import models

class HistoryDescriptor(object):
    def __init__(self, model):
        self.model = model

    def __get__(self, instance, owner):
        if instance is None:
            return HistoryManager(self.model)
        return HistoryManager(self.model, instance)

class HistoryManager(models.Manager):
    def __init__(self, model, instance=None):
        super(HistoryManager, self).__init__()
        self.model = model
        self.instance = instance

    def get_queryset(self):
        if self.instance is None:
            return super(HistoryManager, self).get_queryset()

        filter = {id: self.instance.pk}
        return super(HistoryManager, self).get_queryset().filter(**filter)

    def most_recent(self):
        """
        Returns the most recent copy of the instance available in the history.
        """
        if not self.instance:
            raise TypeError("Can't use most_recent() without a %s instance." % \
                            self.instance._meta.object_name)
        tmp = []
        for field in self.instance._meta.fields:
            if isinstance(field, models.ForeignKey):
                tmp.append(field.name + "_id")
            else:
                tmp.append(field.name)
        fields = tuple(tmp)
        try:
            values = self.values_list(*fields)[0]
        except IndexError:
            raise self.instance.DoesNotExist("%s has no historical record." % \
                                             self.instance._meta.object_name)
        return self.instance.__class__(*values)

    def as_of(self, date):
        """
        Returns an instance of the original model with all the attributes set
        according to what was present on the object on the date provided.
        """
        if not self.instance:
            raise TypeError("Can't use as_of() without a %s instance." % \
                            self.instance._meta.object_name)
        tmp = []
        for field in self.instance._meta.fields:
            if isinstance(field, models.ForeignKey):
                tmp.append(field.name + "_id")
            else:
                tmp.append(field.name)
        fields = tuple(tmp)
        qs = self.filter(history_date__lte=date)
        try:
            values = qs.values_list('history_type', *fields)[0]
        except IndexError:
            raise self.instance.DoesNotExist("%s had not yet been created." % \
                                             self.instance._meta.object_name)
        if values[0] == '-':
            raise self.instance.DoesNotExist("%s had already been deleted." % \
                                             self.instance._meta.object_name)
        return self.instance.__class__(*values[1:])

    def as_of_related(self, history_date):
        """
        Returns an instance of the original model with all the attributes set
        according to what was present on the object on the date provided, and
        all FK relations to SimpleHistory-managed models will also be retrieved
        with their historical versions at the provided date.
        """
        def inject_acessor(instance):
            base_class = instance.__class__
            
            fk_fields = []
            attributes = []
            m2m_fields = getattr(base_class, 'm2m_history_fields', [])
            for field in instance._meta.fields:
                if isinstance(field, models.ForeignKey):
                    fk_fields.append(field.name)
                attr = (field.name, getattr(instance, field.name))
                attributes.append(attr)

            new_base = (base_class,)
            new_name = '%s_as_of_%s_managed' % (base_class.__name__, history_date.strftime("%Y%m%d%H%M%S"))
            #new_name = base_class.__name__

            def getattribute(attr_instance, name):
                overridden_fields = fk_fields
                #if name == 'simple_history_overridden_fields' or name == '__dict__':
                #    return super(attr_instance.__class__, attr_instance).__getattribute__(name)
                if name in overridden_fields:
                    # value = super(attr_instance.__class__, attr_instance).__getattribute__(name)
                    value = base_class.__getattribute__(attr_instance, name)
                    if hasattr(value, 'simple_history_manager') and not hasattr(value, 'as_of_retrieved'):
                        manager = getattr(value, value.simple_history_manager)
                        value = manager.as_of(history_date)
                    if value and not hasattr(value, 'as_of_managed'):
                        value = inject_acessor(value)
                    setattr(value, 'as_of_retrieved', True)
                    return value
                elif name in m2m_fields:
                    # Next six lines based on: http://djangosnippets.org/snippets/236/
                    from django.db.models.sql.compiler import SQLCompiler
                    sql_patched = getattr(SQLCompiler, 'quote_name_unless_alias_patched', False)
                    if not sql_patched:
                        _quote_name_unless_alias = SQLCompiler.quote_name_unless_alias
                        SQLCompiler.quote_name_unless_alias_patched = True
                        SQLCompiler.quote_name_unless_alias = lambda self, name: name if name.startswith('(') else _quote_name_unless_alias(self, name)

                    m2m_class = base_class.__dict__[name].through
                    source_field_name, target_field_name = None, None
                    for field_name, field_value in m2m_class.__dict__.items():
                        if isinstance(field_value, models.fields.related.ReverseSingleRelatedObjectDescriptor):
                            if field_value.field.related.parent_model == instance.__class__:
                                source_field_name = field_name
                            else:
                                target_field_name = field_name

                    db_table = m2m_class.history.model._meta.db_table
                    table = '(select max(history_id) as max_id from %s inner_hm2m'\
                            ' where history_date <= "%s" and %s_id=%s'\
                            ' group by id'\
                            ' order by history_date desc, history_id desc) as top_ids'\
                             % (db_table, history_date.strftime('%Y-%m-%d %H:%M:%S'), source_field_name, instance.pk)
                    conditions = ['top_ids.max_id = %s.history_id' % db_table,
                                  'history_type = "+"']
                    historical_items = m2m_class.history.get_queryset().extra(where=conditions, tables=[table])
                    m2m_item_ids = historical_items.values_list(target_field_name + '_id', flat=True)
                    target_model = base_class.__dict__[name].field.rel.to
                    items = target_model.objects.filter(pk__in=list(m2m_item_ids))
                    return items
                    # TODO: items retrieved through this queryset should also be injected.
                    # Known issue: this will only retrieve target items that haven't been deleted. 
                
                else:
                    #raise AttributeError()
                    return base_class.__getattribute__(attr_instance, name)

            new_dict = {'__getattribute__': getattribute,
                        'as_of_related': True,
                        #'simple_history_overridden_fields': fk_fields,
                        '__module__': instance.__module__,
                        }
            new_class = type(new_name, new_base, new_dict)
            #new_class._meta.db_table = base_class._meta.db_table
            new_class._meta.proxy = True

            new_kwargs = dict(attributes);
            return new_class(**new_kwargs)

        historical_instance = self.as_of(history_date)
        return inject_acessor(historical_instance)

