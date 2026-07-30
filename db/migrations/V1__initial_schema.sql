-- =============================================================================
-- V1__initial_schema.sql
-- First versioned migration for the Sportwide Azure SQL database.
--
-- Migration conventions (Flyway):
--   * File name: V<number>__<description>.sql  (double underscore after number)
--   * Numbers must be unique and increasing: V1, V2, V3, ...
--   * Each migration runs exactly once and is recorded in the
--     flyway_schema_history table that Flyway creates automatically.
--   * NEVER edit a migration that has already been applied to a shared/
--     production database. Add a new V<n> migration instead.
--
-- The tables below are a minimal, editable STARTER schema. Replace them with
-- your real Sportwide model, or delete them before your first apply if you
-- prefer to start empty.
-- =============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE TABLE dbo.Teams
(
    TeamId      INT            IDENTITY(1,1) NOT NULL,
    Name        NVARCHAR(100)  NOT NULL,
    City        NVARCHAR(100)  NULL,
    CreatedAtUtc DATETIME2(0)  NOT NULL CONSTRAINT DF_Teams_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_Teams PRIMARY KEY CLUSTERED (TeamId)
);
GO

CREATE UNIQUE INDEX UX_Teams_Name ON dbo.Teams (Name);
GO

CREATE TABLE dbo.Players
(
    PlayerId    INT            IDENTITY(1,1) NOT NULL,
    TeamId      INT            NULL,
    FirstName   NVARCHAR(100)  NOT NULL,
    LastName    NVARCHAR(100)  NOT NULL,
    JerseyNumber INT           NULL,
    CreatedAtUtc DATETIME2(0)  NOT NULL CONSTRAINT DF_Players_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_Players PRIMARY KEY CLUSTERED (PlayerId),
    CONSTRAINT FK_Players_Teams FOREIGN KEY (TeamId) REFERENCES dbo.Teams (TeamId)
);
GO

CREATE INDEX IX_Players_TeamId ON dbo.Players (TeamId);
GO
