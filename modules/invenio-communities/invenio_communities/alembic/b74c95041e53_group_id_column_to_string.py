#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""group_id_column_to_string"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b74c95041e53'
down_revision = '1b352b00f1ed'
branch_labels = ()
depends_on = 'f2522cdd5fcd'


def upgrade():
    """Upgrade database."""
    op.alter_column(
        'communities_community',
        'id_role',
        existing_type=sa.Integer,
        type_=sa.String(80),
        postgresql_using="id_role::integer",
        existing_nullable=True
    )
    op.create_foreign_key(
        op.f("fk_communities_community_id_role_accounts_role"),
        "communities_community",
        "accounts_role",
        ["id_role"],
        ["id"],
        ondelete="CASCADE",
    )

    op.alter_column(
        'communities_community',
        'group_id',
        existing_type=sa.Integer,
        type_=sa.String(80),
        postgresql_using="group_id::integer",
        existing_nullable=True
    )



def downgrade():
    """Downgrade database."""
    op.alter_column(
        'communities_community',
        'id_role',
        existing_type=sa.String(80),
        type_=sa.Integer,
        postgresql_using="id_role::integer",
        existing_nullable=True
    )
    op.create_foreign_key(
        op.f("fk_communities_community_id_role_accounts_role"),
        "communities_community",
        "accounts_role",
        ["id_role"],
        ["id"],
        ondelete="CASCADE",
    )

    op.alter_column(
        'communities_community',
        'group_id',
        existing_type=sa.String(80),
        type_=sa.Integer,
        postgresql_using="group_id::integer",
        existing_nullable=True
    )
