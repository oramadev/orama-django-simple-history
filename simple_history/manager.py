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

    def get_query_set(self):
        if self.instance is None:
            return super(HistoryManager, self).get_query_set()

        if isinstance(self.instance._meta.pk, models.OneToOneField):
            filter = {self.instance._meta.pk.name+"_id":self.instance.pk}
        else:
            filter = {self.instance._meta.pk.name: self.instance.pk}
        return super(HistoryManager, self).get_query_set().filter(**filter)

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
                tmp.append(field.name+"_id")
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
        tmp=[]
        for field in self.instance._meta.fields:
            if isinstance(field, models.ForeignKey):
                tmp.append(field.name+"_id")
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
    
    def as_of_related(self, date):
        """
        Returns an instance of the original model with all the attributes set
        according to what was present on the object on the date provided, and
        all FK relations to SimpleHistory-managed models will also be retrieved
        with their historical versions at the provided date.
        """
        def inject_acessor(instance):
            fk_fields = [] 
            attributes = []
            for field in instance._meta.fields:
                if isinstance(field, models.ForeignKey):
                    fk_fields.append(field.name)
                attr = (field.name, getattr(instance, field.name))
                attributes.append(attr)
            
            base_class = instance.__class__
            new_base = (base_class,)
            new_name = '%s_as_of_managed' % base_class.__name__
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
                        value = manager.as_of(date)
                    if value and not hasattr(value, 'as_of_managed'):
                        value = inject_acessor(value)
                    setattr(value, 'as_of_retrieved', True)
                    return value
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
            # return new_class.objects.create(**new_kwargs)
        
        historical_instance = self.as_of(date)
        return inject_acessor(historical_instance)

