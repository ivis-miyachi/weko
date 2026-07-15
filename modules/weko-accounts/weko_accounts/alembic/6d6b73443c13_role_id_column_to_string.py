#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""role_id_column_to_string"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6d6b73443c13'
down_revision = 'a8e506eb5e32'
branch_labels = ()
depends_on = 'f2522cdd5fcd'


def upgrade():
    """Upgrade database."""
    op.alter_column(
        'shibboleth_userrole',
        'role_id',
        existing_type=sa.Integer,
        type_=sa.String(80),
        postgresql_using="role_id::integer",
        existing_nullable=True
    )
    op.create_foreign_key(
        op.f("fk_shibboleth_userrole_role_id"),
        "shibboleth_userrole",
        "accounts_role",
        ["role_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade():
    """Downgrade database."""
    op.alter_column(
        'shibboleth_userrole',
        'role_id',
        existing_type=sa.String(80),
        type_=sa.Integer,
        postgresql_using="role_id::integer",
        existing_nullable=True
    )
    op.create_foreign_key(
        op.f("fk_shibboleth_userrole_role_id"),
        "shibboleth_userrole",
        "accounts_role",
        ["role_id"],
        ["id"],
        ondelete="CASCADE",
    )
