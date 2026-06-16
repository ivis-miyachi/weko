"""Records integration for PIDRelations."""

from invenio_pidrelations.api import PIDNodeOrdered

from invenio_pidrelations.utils import resolve_relation_type_config

class WekoPIDNodeDraft(PIDNodeOrdered):

    def __init__(self, pid):
        """Create a record draft API.

        :param pid: either the published record PID or the deposit PID.
        """
        self.relation_type = resolve_relation_type_config('record_draft')
        super(WekoPIDNodeDraft, self).__init__(
            pid=pid, relation_type=self.relation_type,
            max_parents=1, max_children=1
        )
