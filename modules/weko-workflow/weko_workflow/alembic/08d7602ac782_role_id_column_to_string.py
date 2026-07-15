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
revision = '08d7602ac782'
down_revision = 'b1f5618360f5'
branch_labels = ()
depends_on = 'f2522cdd5fcd'


def upgrade():
    """Upgrade database."""
    op.alter_column(
        'workflow_flow_action_role',
        'action_role',
        existing_type=sa.Integer,
        type_=sa.String(80),
        postgresql_using="action_role::integer",
        existing_nullable=True
    )
    op.create_foreign_key(
        op.f("fk_workflow_flow_action_role_action_role_accounts_role"),
        "workflow_flow_action_role",
        "accounts_role",
        ["action_role"],
        ["id"],
        ondelete="CASCADE",
    )

    op.alter_column(
        'workflow_userrole',
        'role_id',
        existing_type=sa.Integer,
        type_=sa.String(80),
        postgresql_using="role_id::integer",
        existing_nullable=True
    )
    op.create_foreign_key(
        op.f("fk_workflow_userrole_role_id_accounts_role"),
        "workflow_userrole",
        "accounts_role",
        ["role_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade():
    """Downgrade database."""
    op.alter_column(
        'workflow_flow_action_role',
        'action_role',
        existing_type=sa.String(80),
        type_=sa.Integer,
        postgresql_using="action_role::integer",
        existing_nullable=True
    )
    op.create_foreign_key(
        op.f("fk_workflow_flow_action_role_action_role_accounts_role"),
        "workflow_flow_action_role",
        "accounts_role",
        ["action_role"],
        ["id"],
        ondelete="CASCADE",
    )

    op.alter_column(
        'workflow_userrole',
        'role_id',
        existing_type=sa.String(80),
        type_=sa.Integer,
        postgresql_using="role_id::integer",
        existing_nullable=True
    )
    op.create_foreign_key(
        op.f("fk_workflow_userrole_role_id_accounts_role"),
        "workflow_userrole",
        "accounts_role",
        ["role_id"],
        ["id"],
        ondelete="CASCADE",
    )
