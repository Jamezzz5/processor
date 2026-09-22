"""Add the social-signal lanes' storage: daily advertiser-targeting
query evidence and the Discord lane's own liveness column.

Revision ID: 9a6e4207c8d1
Revises: a5c7e9b1d3f5
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = '9a6e4207c8d1'
down_revision = 'a5c7e9b1d3f5'
branch_labels = None
depends_on = None


def upgrade():
    """Keep query subjects separate from suggested keyword identities."""
    op.create_table(
        'targeting_snapshot',
        sa.Column('targetingsnapshotid', sa.BigInteger(),
                  primary_key=True),
        sa.Column('provider', sa.Text(), nullable=False,
                  comment="'tiktok' | 'meta' - the advertising platform "
                          'whose catalogue answered.'),
        sa.Column('account_key', sa.Text(), nullable=False),
        sa.Column('subject_key', sa.Text(), nullable=False),
        sa.Column('queried_gameid', sa.BigInteger(),
                  sa.ForeignKey('games.game.gameid')),
        sa.Column('title', sa.Text()),
        sa.Column('source', sa.Text(), nullable=False),
        sa.Column('query_text', sa.Text(), nullable=False),
        sa.Column('snapshot_date', sa.Date(), nullable=False),
        sa.Column('collected_at', sa.DateTime(), nullable=False),
        sa.Column('coverage', sa.Text(), nullable=False),
        sa.Column('evidence', sa.JSON().with_variant(
            postgresql.JSONB(), 'postgresql'), nullable=False,
                  comment='API query scope, status and returned '
                          'entries. Related suggestions are not '
                          'attributed to the query game. No post '
                          'counts or views inferred; Meta audience '
                          'bounds are the platform\'s own estimate.'),
        sa.UniqueConstraint(
            'provider', 'account_key', 'subject_key', 'source',
            'query_text', 'snapshot_date',
            name='uq_targeting_snapshot_query_day'),
        schema='games',
    )
    op.create_index('ix_targeting_snapshot_date', 'targeting_snapshot',
                    ['snapshot_date'], schema='games')
    op.create_index('ix_targeting_snapshot_game', 'targeting_snapshot',
                    ['queried_gameid'], schema='games')
    # discord_members is shared with the registry sheet; the live lane
    # needs a column only it writes.
    op.add_column(
        'community_snapshot',
        sa.Column('discord_online', sa.Numeric(),
                  comment='Members online on the linked Discord server '
                          '(invite approximate_presence_count) at the '
                          'nightly sample - a point-in-time count.'),
        schema='games')


def downgrade():
    """Remove only the targeting table and the Discord online column."""
    op.drop_column('community_snapshot', 'discord_online',
                   schema='games')
    op.drop_table('targeting_snapshot', schema='games')
