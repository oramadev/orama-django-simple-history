import copy
import datetime
from django.db import models
from django.contrib import admin
from django.contrib.auth.models import User
from django.utils import importlib
from manager import HistoryDescriptor
import simple_history

class HistoricalRecords(object):
    def contribute_to_class(self, cls, name):
        self.manager_name = name
        self.module = cls.__module__
        models.signals.class_prepared.connect(self.finalize, sender=cls)
        setattr(cls, 'simple_history_manager', name)

        def save_without_historical_record(self, *args, **kwargs):
            """Caution! Make sure you know what you're doing before you use this method."""
            self.skip_history_when_saving = True
            ret = self.save(*args, **kwargs)
            del self.skip_history_when_saving
            return ret
        setattr(cls, 'save_without_historical_record', save_without_historical_record)

        # Injecting HistoricalRecords into ManyToManyFields' intermediate tables ('through' models)
        if hasattr(cls, 'm2m_history_fields'):
            m2m_history_fields = getattr(cls, 'm2m_history_fields', None)
            assert (isinstance(m2m_history_fields, list)
                or  isinstance(m2m_history_fields, tuple)), 'm2m_history_fields must be a list or tuple'
            for field_name in m2m_history_fields:
                field = getattr(cls, field_name).field
                assert isinstance(field, models.fields.related.ManyToManyField), ('%s must be a ManyToManyField' % field_name)
                if not sum([isinstance(item, HistoricalRecords) for item in field.rel.through.__dict__.values()]):
                    field.rel.through.history = HistoricalRecords()
                    simple_history.register(field.rel.through)

    def finalize(self, sender, **kwargs):
        history_model = self.create_history_model(sender)
        module = importlib.import_module(self.module)
        setattr(module, history_model.__name__, history_model)

        # The HistoricalRecords object will be discarded,
        # so the signal handlers can't use weak references.
        models.signals.post_save.connect(self.post_save, sender=sender,
                                         weak=False)
        models.signals.post_delete.connect(self.post_delete, sender=sender,
                                           weak=False)
        models.signals.m2m_changed.connect(self.m2m_changed, sender=sender,
                                           weak=False)

        descriptor = HistoryDescriptor(history_model)
        setattr(sender, self.manager_name, descriptor)
        sender._meta.simple_history_manager_attribute = self.manager_name

    def create_history_model(self, model):
        """
        Creates a historical model to associate with the model provided.
        """
        attrs = {'__module__': self.module}

        fields = self.copy_fields(model)
        attrs.update(fields)
        attrs.update(self.get_extra_fields(model, fields))
        attrs.update(Meta=type('Meta', (), self.get_meta_options(model)))
        name = 'Historical%s' % model._meta.object_name
        return type(name, (models.Model,), attrs)

    def copy_fields(self, model):
        """
        Creates copies of the model's original fields, returning
        a dictionary mapping field name to copied field object.
        """
        fields = {}

        for field in model._meta.fields:
            field = copy.copy(field)
            fk = None

            if isinstance(field, models.AutoField):
                # The historical model gets its own AutoField, so any
                # existing one must be replaced with an IntegerField.
                field.__class__ = models.IntegerField

            if isinstance(field, models.ForeignKey):
                field.__class__ = models.IntegerField
                #ughhhh. open to suggestions here
                try:
                    field.rel = None
                except:
                    pass
                try:
                    field.related = None
                except:
                    pass
                try:
                    field.related_query_name = None
                except:
                    pass
                field.null = True
                field.blank = True
                fk = True
            else:
                fk = False

            # The historical instance should not change creation/modification timestamps.
            field.auto_now = False
            field.auto_now_add = False

            if field.primary_key or field.unique:
                # Unique fields can no longer be guaranteed unique,
                # but they should still be indexed for faster lookups.
                field.primary_key = False
                field._unique = False
                field.db_index = True
            if fk:
                field.name = field.name + "_id"
            fields[field.name] = field

        return fields

    def get_extra_fields(self, model, fields):
        """
        Returns a dictionary of fields that will be added to the historical
        record model, in addition to the ones returned by copy_fields below.
        """
        @models.permalink
        def revert_url(self):
            opts = model._meta
            return ('%s:%s_%s_simple_history' %
                    (admin.site.name, opts.app_label, opts.module_name),
                    [getattr(self, opts.pk.attname), self.history_id])
        def get_instance(self):
            return model(**dict([(k, getattr(self, k)) for k in fields]))

        rel_nm = '_%s_history' % model._meta.object_name.lower()
        return {
            'history_id': models.AutoField(primary_key=True),
            'history_date': models.DateTimeField(default=datetime.datetime.now),
            'history_type': models.CharField(max_length=1, choices=(
                ('+', 'Created'),
                ('~', 'Changed'),
                ('-', 'Deleted'),
            )),
            'history_object': HistoricalObjectDescriptor(model),
            'changed_by': models.ForeignKey(User, null=True),
            'instance': property(get_instance),
            'revert_url': revert_url,
            '__unicode__': lambda self: u'%s as of %s' % (self.history_object,
                                                          self.history_date)
        }

    def get_meta_options(self, model):
        """
        Returns a dictionary of fields that will be added to
        the Meta inner class of the historical record model.
        """
        meta_options = {
            'ordering': ('-history_date', '-history_id'),
        }
        if hasattr(model._meta, 'app_label'):
            meta_options['app_label'] = model._meta.app_label
        return meta_options

    def post_save(self, instance, created, **kwargs):
        if not created and hasattr(instance, 'skip_history_when_saving'):
            return
        self.create_historical_record(instance, created and '+' or '~')

    def post_delete(self, instance, **kwargs):
        self.create_historical_record(instance, '-')

    def m2m_changed(self, action, instance, sender, **kwargs):
        source_field_name, target_field_name = None, None
        for field_name, field_value in sender.__dict__.items():
            if isinstance(field_value, models.fields.related.ReverseSingleRelatedObjectDescriptor):
                if field_value.field.related.parent_model == kwargs['model']:
                    target_field_name = field_name
                elif field_value.field.related.parent_model == type(instance):
                    source_field_name = field_name
        items = sender.objects.filter(**{source_field_name:instance})
        if kwargs['pk_set']:
            items = items.filter(**{target_field_name + '__id__in':kwargs['pk_set']})
        for item in items:
            if action == 'post_add':
                if hasattr(item, 'skip_history_when_saving'):
                    return
                self.create_historical_record(item, '+')
            elif action == 'pre_remove':
                self.create_historical_record(item, '-')
            elif action == 'pre_clear':
                self.create_historical_record(item, '-')

    def create_historical_record(self, instance, type):
        changed_by = getattr(instance, '_changed_by_user', None)
        manager = getattr(instance, self.manager_name)
        attrs = {}
        for field in instance._meta.fields:
            attrs[field.attname] = getattr(instance, field.attname)
        manager.create(history_type=type, changed_by=changed_by, **attrs)

class HistoricalObjectDescriptor(object):
    def __init__(self, model):
        self.model = model

    def __get__(self, instance, owner):
        values = (getattr(instance, f.attname) for f in self.model._meta.fields)
        return self.model(*values)
