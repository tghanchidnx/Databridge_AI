import {
  IsString,
  IsObject,
  IsArray,
  IsOptional,
  IsEnum,
  IsNumber,
  IsBoolean,
  ValidateNested,
  IsNotEmpty,
  Allow,
  MinLength,
} from 'class-validator';
import { Type, Transform } from 'class-transformer';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

// ============================================================================
// Project Management DTOs
// ============================================================================

export class CreateProjectDto {
  @ApiProperty({ description: 'Project name' })
  @IsString()
  @IsNotEmpty()
  name: string;

  @ApiPropertyOptional({ description: 'Project description' })
  @IsOptional()
  @IsString()
  description?: string;

  @ApiPropertyOptional({ description: 'User ID (will be set from auth context)' })
  @IsOptional()
  @IsString()
  userId?: string;

  @ApiPropertyOptional({ description: 'Organization ID' })
  @IsOptional()
  @IsString()
  organizationId?: string;
}

export class UpdateProjectDto {
  @ApiPropertyOptional({ description: 'Project name' })
  @IsOptional()
  @IsString()
  name?: string;

  @ApiPropertyOptional({ description: 'Project description' })
  @IsOptional()
  @IsString()
  description?: string;

  @ApiPropertyOptional({ description: 'Is project active' })
  @IsOptional()
  @IsBoolean()
  isActive?: boolean;
}

export class BulkDeleteHierarchiesDto {
  @ApiProperty({
    description: 'Array of hierarchy IDs to delete',
    type: [String],
    example: ['TOTAL_REVENUE_1', 'OPERATING_EXPENSES_1'],
  })
  @IsArray()
  @IsString({ each: true })
  @IsNotEmpty()
  hierarchyIds: string[];
}

// ============================================================================
// Nested DTOs for JSON structures
// ============================================================================

export class HierarchyLevelDto {
  @ApiPropertyOptional() @IsOptional() @IsString() level_1?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() level_1_sort?: number;
  @ApiPropertyOptional() @IsOptional() @IsString() level_2?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() level_2_sort?: number;
  @ApiPropertyOptional() @IsOptional() @IsString() level_3?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() level_3_sort?: number;
  @ApiPropertyOptional() @IsOptional() @IsString() level_4?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() level_4_sort?: number;
  @ApiPropertyOptional() @IsOptional() @IsString() level_5?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() level_5_sort?: number;
  @ApiPropertyOptional() @IsOptional() @IsString() level_6?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() level_6_sort?: number;
  @ApiPropertyOptional() @IsOptional() @IsString() level_7?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() level_7_sort?: number;
  @ApiPropertyOptional() @IsOptional() @IsString() level_8?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() level_8_sort?: number;
  @ApiPropertyOptional() @IsOptional() @IsString() level_9?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() level_9_sort?: number;
  @ApiPropertyOptional() @IsOptional() @IsString() level_10?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() level_11?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() level_12?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() level_13?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() level_14?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() level_15?: string;
}

export class HierarchyFlagsDto {
  @ApiProperty()
  @Transform(({ value }) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') return value.toLowerCase() === 'true';
    if (typeof value === 'number') return value !== 0;
    return Boolean(value);
  })
  @IsBoolean()
  include_flag: boolean;

  @ApiProperty()
  @Transform(({ value }) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') return value.toLowerCase() === 'true';
    if (typeof value === 'number') return value !== 0;
    return Boolean(value);
  })
  @IsBoolean()
  exclude_flag: boolean;

  @ApiProperty()
  @Transform(({ value }) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') return value.toLowerCase() === 'true';
    if (typeof value === 'number') return value !== 0;
    return Boolean(value);
  })
  @IsBoolean()
  transform_flag: boolean;

  @ApiPropertyOptional({ default: false })
  @IsOptional()
  @Transform(({ value }) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') return value.toLowerCase() === 'true';
    if (typeof value === 'number') return value !== 0;
    return Boolean(value);
  })
  @IsBoolean()
  calculation_flag?: boolean;

  @ApiProperty()
  @Transform(({ value }) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') return value.toLowerCase() === 'true';
    if (typeof value === 'number') return value !== 0;
    return Boolean(value);
  })
  @IsBoolean()
  active_flag: boolean;

  @ApiProperty()
  @Transform(({ value }) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') return value.toLowerCase() === 'true';
    if (typeof value === 'number') return value !== 0;
    return Boolean(value);
  })
  @IsBoolean()
  is_leaf_node: boolean;

  // Custom dynamic flags - allows adding any additional boolean flags
  @ApiPropertyOptional({ type: 'object', additionalProperties: { type: 'boolean' } })
  @IsOptional()
  @IsObject()
  customFlags?: Record<string, boolean>;
}

export class SourceMappingFlagsDto {
  @ApiProperty()
  @Transform(({ value }) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') return value.toLowerCase() === 'true';
    if (typeof value === 'number') return value !== 0;
    return Boolean(value);
  })
  @IsBoolean()
  include_flag: boolean;

  @ApiProperty()
  @Transform(({ value }) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') return value.toLowerCase() === 'true';
    if (typeof value === 'number') return value !== 0;
    return Boolean(value);
  })
  @IsBoolean()
  exclude_flag: boolean;

  @ApiProperty()
  @Transform(({ value }) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') return value.toLowerCase() === 'true';
    if (typeof value === 'number') return value !== 0;
    return Boolean(value);
  })
  @IsBoolean()
  transform_flag: boolean;

  @ApiProperty()
  @Transform(({ value }) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') return value.toLowerCase() === 'true';
    if (typeof value === 'number') return value !== 0;
    return Boolean(value);
  })
  @IsBoolean()
  active_flag: boolean;

  // Custom dynamic flags - allows adding any additional boolean flags
  @ApiPropertyOptional({ type: 'object', additionalProperties: { type: 'boolean' } })
  @IsOptional()
  @IsObject()
  customFlags?: Record<string, boolean>;
}

// FP&A: Join key configuration for multi-column joins
export class JoinKeyConfigDto {
  @ApiProperty({ description: 'Source column name' })
  @IsString()
  source_column: string;

  @ApiProperty({ description: 'Target column name' })
  @IsString()
  target_column: string;

  @ApiPropertyOptional({
    description: 'Join operator',
    enum: ['=', 'LIKE', 'IN'],
    default: '='
  })
  @IsOptional()
  @IsEnum(['=', 'LIKE', 'IN'])
  operator?: '=' | 'LIKE' | 'IN';
}

export class SourceMappingDto {
  @ApiProperty() @IsNumber() mapping_index: number;
  @ApiProperty() @IsString() source_database: string;
  @ApiProperty() @IsString() source_schema: string;
  @ApiProperty() @IsString() source_table: string;
  @ApiProperty() @IsString() source_column: string;
  @ApiPropertyOptional() @IsOptional() @IsString() source_uid?: string;
  @ApiPropertyOptional({ description: 'Column in hierarchy view to join on (e.g., XREF_HIERARCHY_KEY)' })
  @IsOptional()
  @IsString()
  hierarchy_key_column?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() precedence_group?: string;

  // FP&A: Join type for fact table generation
  @ApiPropertyOptional({
    description: 'Join type for fact table generation',
    enum: ['INNER', 'LEFT', 'RIGHT', 'FULL'],
    default: 'LEFT'
  })
  @IsOptional()
  @IsEnum(['INNER', 'LEFT', 'RIGHT', 'FULL'])
  join_type?: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';

  // FP&A: System type for multi-system comparison
  @ApiPropertyOptional({
    description: 'System type for FP&A comparison',
    enum: ['ACTUALS', 'BUDGET', 'FORECAST', 'PRIOR_YEAR', 'CUSTOM'],
    default: 'ACTUALS'
  })
  @IsOptional()
  @IsEnum(['ACTUALS', 'BUDGET', 'FORECAST', 'PRIOR_YEAR', 'CUSTOM'])
  system_type?: 'ACTUALS' | 'BUDGET' | 'FORECAST' | 'PRIOR_YEAR' | 'CUSTOM';

  // FP&A: Dimension role for join prioritization
  @ApiPropertyOptional({
    description: 'Dimension role for join prioritization',
    enum: ['PRIMARY', 'SECONDARY', 'OPTIONAL'],
    default: 'SECONDARY'
  })
  @IsOptional()
  @IsEnum(['PRIMARY', 'SECONDARY', 'OPTIONAL'])
  dimension_role?: 'PRIMARY' | 'SECONDARY' | 'OPTIONAL';

  // FP&A: Fact table reference
  @ApiPropertyOptional({ description: 'Reference to fact table' })
  @IsOptional()
  @IsString()
  fact_table_ref?: string;

  // FP&A: Multi-column join keys
  @ApiPropertyOptional({ description: 'Join keys for multi-column joins', type: [JoinKeyConfigDto] })
  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => JoinKeyConfigDto)
  join_keys?: JoinKeyConfigDto[];

  @ApiProperty({ type: SourceMappingFlagsDto })
  @ValidateNested()
  @Type(() => SourceMappingFlagsDto)
  flags: SourceMappingFlagsDto;

  // Old format fields (from legacy CSV import) - optional for backward compatibility
  @ApiPropertyOptional() @IsOptional() @IsString() id_name?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() id?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() id_source?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() id_table?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() id_schema?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() id_database?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() level_node?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() hierarchy_name?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() sort_order?: number;
  @ApiPropertyOptional() @IsOptional() @IsNumber() group_filter_precedence?: number;
  @ApiPropertyOptional() @IsOptional() @IsObject() filter_groups?: Record<string, any>;
  @ApiPropertyOptional() @IsOptional() @IsObject() formula_info?: Record<string, any>;
}

export class FormulaGroupReferenceDto {
  @ApiProperty() @IsString() formulaGroupId: string;
  @ApiPropertyOptional({ enum: ['MAIN', 'CONTRIBUTOR'] })
  @IsOptional()
  @IsEnum(['MAIN', 'CONTRIBUTOR'])
  role?: 'MAIN' | 'CONTRIBUTOR';
  @ApiPropertyOptional() @IsOptional() @IsNumber() FORMULA_PRECEDENCE?: number;
  @ApiPropertyOptional() @IsOptional() @IsString() FORMULA_PARAM_REF?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() FORMULA_PARAM2_CONST_NUMBER?: number;
}

export class FormulaConfigDto {
  @ApiPropertyOptional({ enum: ['SQL', 'EXPRESSION', 'AGGREGATE'] })
  @IsOptional()
  @IsEnum(['SQL', 'EXPRESSION', 'AGGREGATE'])
  formula_type?: 'SQL' | 'EXPRESSION' | 'AGGREGATE';

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  formula_text?: string;

  @ApiPropertyOptional() @IsOptional() @IsObject() variables?: Record<string, any>;
  @ApiPropertyOptional({ type: FormulaGroupReferenceDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => FormulaGroupReferenceDto)
  formula_group_ref?: FormulaGroupReferenceDto;

  @ApiPropertyOptional()
  @IsOptional()
  @IsObject()
  formula_group?: {
    mainHierarchyId: string;
    mainHierarchyName: string;
    rules: Array<{
      hierarchyId: string;
      hierarchyName: string;
      operation: string;
      precedence: number;
      parameterReference?: string;
      constantNumber?: number;
    }>;
  };
}

export class FilterConditionDto {
  @ApiProperty() @IsString() column: string;
  @ApiProperty() @IsString() operator: string;
  @ApiPropertyOptional() @IsOptional() value?: any;
  @ApiPropertyOptional({ enum: ['AND', 'OR'] })
  @IsOptional()
  @IsEnum(['AND', 'OR'])
  logic?: 'AND' | 'OR';
}

export class FilterGroupReferenceDto {
  @ApiProperty() @IsString() filterGroupId: string;
  @ApiProperty() @IsString() filterGroupName: string;
}

export class FilterConfigDto {
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_1?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_1_type?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_2?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_2_type?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_3?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_3_type?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_4?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_4_type?: string;

  @ApiProperty({ type: [FilterConditionDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => FilterConditionDto)
  filter_conditions: FilterConditionDto[];

  @ApiPropertyOptional() @IsOptional() @IsString() custom_sql?: string;
  @ApiPropertyOptional({ type: FilterGroupReferenceDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => FilterGroupReferenceDto)
  filter_group_ref?: FilterGroupReferenceDto;

  @ApiPropertyOptional()
  @IsOptional()
  @IsObject()
  total_formula?: {
    mainHierarchyId: string;
    mainHierarchyName: string;
    aggregation: 'SUM' | 'AVERAGE' | 'COUNT' | 'MIN' | 'MAX';
    children: Array<{
      hierarchyId: string;
      hierarchyName: string;
    }>;
  };
}

export class PivotConfigDto {
  @ApiProperty({ type: [String] }) @IsArray() @IsString({ each: true }) pivot_columns: string[];
  @ApiProperty() @IsObject() aggregate_functions: Record<string, string>;
}

// ============================================================================
// Formula Group & Filter Group DTOs
// ============================================================================

export class FormulaRuleDto {
  @ApiProperty({ enum: ['SUM', 'SUBTRACT', 'MULTIPLY', 'DIVIDE', 'AVERAGE'] })
  @IsEnum(['SUM', 'SUBTRACT', 'MULTIPLY', 'DIVIDE', 'AVERAGE'])
  operation: 'SUM' | 'SUBTRACT' | 'MULTIPLY' | 'DIVIDE' | 'AVERAGE';

  @ApiProperty() @IsString() hierarchyId: string;
  @ApiProperty() @IsString() hierarchyName: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() FORMULA_PRECEDENCE?: number;
  @ApiPropertyOptional() @IsOptional() @IsString() FORMULA_PARAM_REF?: string;
  @ApiPropertyOptional() @IsOptional() @IsNumber() FORMULA_PARAM2_CONST_NUMBER?: number;
}

// Old formula group DTOs - being replaced with new approach
// Kept temporarily for compatibility
export class OldFormulaGroupDto {
  @ApiProperty() @IsString() @IsNotEmpty() groupName: string;
  @ApiProperty() @IsString() @IsNotEmpty() mainHierarchy: string;
  @ApiProperty() @IsString() @IsNotEmpty() mainHierarchyId: string;
  @ApiProperty({ type: [FormulaRuleDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => FormulaRuleDto)
  rules: FormulaRuleDto[];
  @ApiPropertyOptional() @IsOptional() @IsObject() formula_params?: Record<string, any>;
}

export class UpdateOldFormulaGroupDto extends OldFormulaGroupDto {
  @ApiProperty() @IsString() @IsNotEmpty() id: string;
}

export class CreateFilterGroupDto {
  @ApiProperty() @IsString() @IsNotEmpty() groupName: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_1?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_1_type?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_2?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_2_type?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_3?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_3_type?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_4?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() filter_group_4_type?: string;
  @ApiProperty({ type: [FilterConditionDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => FilterConditionDto)
  filter_conditions: FilterConditionDto[];
  @ApiPropertyOptional() @IsOptional() @IsString() custom_sql?: string;
  @ApiPropertyOptional({ type: [String] })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  assignedHierarchies?: string[];
}

export class UpdateFilterGroupDto extends CreateFilterGroupDto {
  @ApiProperty() @IsString() @IsNotEmpty() id: string;
}

// ============================================================================
// Total Formula DTOs
// ============================================================================

export class TotalFormulaChildDto {
  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  @MinLength(1)
  hierarchyId: string;

  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  @MinLength(1)
  hierarchyName: string;

  @ApiPropertyOptional()
  @IsOptional()
  @IsNumber()
  level?: number;
}

export class CreateTotalFormulaDto {
  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MinLength(1)
  mainHierarchyId?: string;

  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  @MinLength(1)
  mainHierarchyName: string;

  @ApiProperty({ enum: ['SUM', 'AVERAGE', 'COUNT', 'MIN', 'MAX'] })
  @IsEnum(['SUM', 'AVERAGE', 'COUNT', 'MIN', 'MAX'])
  aggregation: 'SUM' | 'AVERAGE' | 'COUNT' | 'MIN' | 'MAX';

  @ApiProperty({ type: [TotalFormulaChildDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => TotalFormulaChildDto)
  children: TotalFormulaChildDto[];
}

export class UpdateTotalFormulaDto {
  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MinLength(1)
  mainHierarchyId?: string;

  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  @MinLength(1)
  mainHierarchyName: string;

  @ApiProperty({ enum: ['SUM', 'AVERAGE', 'COUNT', 'MIN', 'MAX'] })
  @IsEnum(['SUM', 'AVERAGE', 'COUNT', 'MIN', 'MAX'])
  aggregation: 'SUM' | 'AVERAGE' | 'COUNT' | 'MIN' | 'MAX';

  @ApiProperty({ type: [TotalFormulaChildDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => TotalFormulaChildDto)
  children: TotalFormulaChildDto[];
}

// ============================================================================
// Formula Group DTOs (similar to Total Formula but stores in formulaConfig)
// ============================================================================

export class FormulaGroupRuleDto {
  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  @MinLength(1)
  hierarchyId: string;

  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  @MinLength(1)
  hierarchyName: string;

  @ApiProperty({
    enum: ['ADD', 'SUBTRACT', 'MULTIPLY', 'DIVIDE', 'SUM', 'AVERAGE', 'COUNT', 'MIN', 'MAX'],
  })
  @IsEnum(['ADD', 'SUBTRACT', 'MULTIPLY', 'DIVIDE', 'SUM', 'AVERAGE', 'COUNT', 'MIN', 'MAX'])
  operation:
    | 'ADD'
    | 'SUBTRACT'
    | 'MULTIPLY'
    | 'DIVIDE'
    | 'SUM'
    | 'AVERAGE'
    | 'COUNT'
    | 'MIN'
    | 'MAX';

  @ApiProperty()
  @IsNumber()
  precedence: number;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  parameterReference?: string;

  @ApiPropertyOptional()
  @IsOptional()
  @IsNumber()
  constantNumber?: number;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  formulaRefSource?: string;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  formulaRefTable?: string;
}

export class CreateFormulaGroupDto {
  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MinLength(1)
  mainHierarchyId?: string;

  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  @MinLength(1)
  mainHierarchyName: string;

  @ApiProperty({ type: [FormulaGroupRuleDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => FormulaGroupRuleDto)
  rules: FormulaGroupRuleDto[];
}

export class UpdateFormulaGroupDto {
  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MinLength(1)
  mainHierarchyId?: string;

  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  @MinLength(1)
  mainHierarchyName: string;

  @ApiProperty({ type: [FormulaGroupRuleDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => FormulaGroupRuleDto)
  rules: FormulaGroupRuleDto[];
}

// ============================================================================
// Main DTOs
// ============================================================================

export class CreateSmartHierarchyDto {
  @ApiProperty() @IsString() @IsNotEmpty() projectId: string;
  @ApiProperty() @IsString() @IsNotEmpty() hierarchyId: string;
  @ApiProperty() @IsString() @IsNotEmpty() hierarchyName: string;
  @ApiPropertyOptional() @IsOptional() @IsString() description?: string;

  // Tree structure fields
  @ApiPropertyOptional() @IsOptional() @IsString() parentId?: string;
  @ApiPropertyOptional() @IsOptional() @IsBoolean() isRoot?: boolean;
  @ApiPropertyOptional() @IsOptional() @IsNumber() sortOrder?: number;

  @ApiPropertyOptional({ type: HierarchyLevelDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => HierarchyLevelDto)
  hierarchyLevel?: HierarchyLevelDto;

  @ApiProperty({ type: HierarchyFlagsDto })
  @ValidateNested()
  @Type(() => HierarchyFlagsDto)
  flags: HierarchyFlagsDto;

  @ApiProperty({ type: [SourceMappingDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => SourceMappingDto)
  mapping: SourceMappingDto[];

  @ApiPropertyOptional({ type: FormulaConfigDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => FormulaConfigDto)
  formulaConfig?: FormulaConfigDto;

  @ApiPropertyOptional({ type: FilterConfigDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => FilterConfigDto)
  filterConfig?: FilterConfigDto;

  @ApiPropertyOptional({ type: PivotConfigDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => PivotConfigDto)
  pivotConfig?: PivotConfigDto;

  @ApiPropertyOptional() @IsOptional() @IsObject() metadata?: Record<string, any>;
  @ApiPropertyOptional() @IsOptional() @IsString() createdBy?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() updatedBy?: string;
}

export class UpdateSmartHierarchyDto {
  // NOTE: Read-only fields (id, projectId, hierarchyId, createdBy, createdAt, updatedAt) should NOT be included in update
  // These are managed by the system and should not be modified by the client

  @ApiPropertyOptional() @IsOptional() @IsString() hierarchyName?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() description?: string;

  // Tree structure fields
  @ApiPropertyOptional() @IsOptional() @IsString() parentId?: string;
  @ApiPropertyOptional() @IsOptional() @IsBoolean() isRoot?: boolean;
  @ApiPropertyOptional() @IsOptional() @IsNumber() sortOrder?: number;

  @ApiPropertyOptional({ type: HierarchyLevelDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => HierarchyLevelDto)
  hierarchyLevel?: HierarchyLevelDto;

  @ApiPropertyOptional({ type: HierarchyFlagsDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => HierarchyFlagsDto)
  flags?: HierarchyFlagsDto;

  @ApiPropertyOptional({ type: [SourceMappingDto] })
  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => SourceMappingDto)
  mapping?: SourceMappingDto[];

  @ApiPropertyOptional({ type: FormulaConfigDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => FormulaConfigDto)
  formulaConfig?: FormulaConfigDto;

  @ApiPropertyOptional({ type: FilterConfigDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => FilterConfigDto)
  filterConfig?: FilterConfigDto;

  @ApiPropertyOptional({ type: PivotConfigDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => PivotConfigDto)
  pivotConfig?: PivotConfigDto;

  @ApiPropertyOptional() @IsOptional() @IsObject() metadata?: Record<string, any>;
  @ApiPropertyOptional() @IsOptional() @IsString() updatedBy?: string;
}

export class ExportProjectDto {
  @ApiProperty() @IsString() @IsNotEmpty() projectId: string;
  @ApiProperty() @IsString() @IsNotEmpty() exportName: string;
  @ApiPropertyOptional() @IsOptional() @IsString() description?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() version?: string;
  @ApiPropertyOptional() @IsOptional() @IsString() createdBy?: string;
  @ApiPropertyOptional({
    enum: ['manual', 'auto-backup', 'snapshot'],
    default: 'manual',
  })
  @IsOptional()
  @IsEnum(['manual', 'auto-backup', 'snapshot'])
  exportType?: 'manual' | 'auto-backup' | 'snapshot';

  @ApiPropertyOptional({
    enum: ['json', 'csv'],
    default: 'json',
  })
  @IsOptional()
  @IsEnum(['json', 'csv'])
  format?: 'json' | 'csv';
}

export class ImportProjectDto {
  @ApiProperty()
  @IsString()
  @IsNotEmpty()
  projectId: string;

  @ApiProperty({
    description: 'Export data - JSON object for json format or CSV string for csv format',
    type: 'object',
  })
  @Allow()
  exportData: any; // JSON object or CSV string

  @ApiPropertyOptional({
    enum: ['json', 'csv'],
    default: 'json',
  })
  @IsOptional()
  @IsEnum(['json', 'csv'])
  format?: 'json' | 'csv';
}

export class GenerateScriptDto {
  @ApiProperty() @IsString() @IsNotEmpty() projectId: string;

  @ApiPropertyOptional({ type: [String] })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  hierarchyIds?: string[]; // If not provided, generate for all hierarchies

  @ApiProperty({ enum: ['insert', 'view', 'mapping', 'dt', 'all'] })
  @IsEnum(['insert', 'view', 'mapping', 'dt', 'all'])
  scriptType: 'insert' | 'view' | 'mapping' | 'dt' | 'all';

  @ApiProperty({
    enum: ['snowflake', 'postgres', 'mysql', 'sqlserver', 'all'],
    default: 'snowflake',
  })
  @IsEnum(['snowflake', 'postgres', 'mysql', 'sqlserver', 'all'])
  databaseType: 'snowflake' | 'postgres' | 'mysql' | 'sqlserver' | 'all';

  @ApiPropertyOptional({ description: 'User email for UPDATED_BY field' })
  @IsOptional()
  @IsString()
  deployedBy?: string;

  @ApiPropertyOptional({ description: 'Target database for qualified names' })
  @IsOptional()
  @IsString()
  database?: string;

  @ApiPropertyOptional({ description: 'Target schema for qualified names' })
  @IsOptional()
  @IsString()
  schema?: string;
}

export class PushToSnowflakeDto {
  @ApiProperty() @IsString() @IsNotEmpty() projectId: string;
  @ApiPropertyOptional() @IsOptional() @IsArray() hierarchyIds?: string[]; // Changed to array - only selected hierarchies
  @ApiProperty() @IsString() @IsNotEmpty() connectionId: string;
  @ApiProperty() @IsString() @IsNotEmpty() database: string;
  @ApiProperty() @IsString() @IsNotEmpty() schema: string;
  @ApiProperty() @IsString() @IsNotEmpty() masterTableName: string; // Added
  @ApiProperty() @IsString() @IsNotEmpty() masterViewName: string; // Added
  @ApiProperty() @IsEnum(['snowflake', 'postgres', 'mysql', 'sqlserver']) databaseType:
    | 'snowflake'
    | 'postgres'
    | 'mysql'
    | 'sqlserver'; // Added
  @ApiProperty() @IsString() @IsNotEmpty() deployedBy: string; // User email
  @ApiPropertyOptional({ default: false }) @IsOptional() @IsBoolean() createTables?: boolean;
  @ApiPropertyOptional({ default: true }) @IsOptional() @IsBoolean() createViews?: boolean;
  @ApiPropertyOptional({ default: false }) @IsOptional() @IsBoolean() createDynamicTables?: boolean;
  @ApiPropertyOptional({ default: false })
  @IsOptional()
  @IsBoolean()
  saveAsDeploymentConfig?: boolean; // Save to project config
}

// Deployment History Response DTO
export class DeploymentHistoryDto {
  @ApiProperty() id: string;
  @ApiProperty() projectId: string;
  @ApiProperty() connectionId: string;
  @ApiProperty() database: string;
  @ApiProperty() schema: string;
  @ApiProperty() masterTableName: string;
  @ApiProperty() masterViewName: string;
  @ApiProperty() databaseType: string;
  @ApiProperty() hierarchyIds: string[];
  @ApiPropertyOptional() hierarchyNames?: string[];
  @ApiProperty() deployedBy: string;
  @ApiProperty() deployedAt: Date;
  @ApiProperty() status: string;
  @ApiProperty() successCount: number;
  @ApiProperty() failedCount: number;
  @ApiPropertyOptional() errorMessage?: string;
  @ApiPropertyOptional() executionTime?: number;
  @ApiPropertyOptional() insertScript?: string;
  @ApiPropertyOptional() viewScript?: string;
  @ApiPropertyOptional() mappingScript?: string;
  @ApiPropertyOptional() dynamicTableScript?: string;
}

// Deployment Config Response DTO
export class DeploymentConfigDto {
  @ApiProperty() connectionId: string;
  @ApiProperty() database: string;
  @ApiProperty() schema: string;
  @ApiProperty() masterTableName: string;
  @ApiProperty() masterViewName: string;
  @ApiProperty() databaseType: string;
  @ApiProperty() createTables: boolean;
  @ApiProperty() createViews: boolean;
  @ApiProperty() createDynamicTables: boolean;
}
// Bulk Update Order DTO
export class HierarchyOrderUpdate {
  @ApiProperty() @IsString() @IsNotEmpty() hierarchyId: string;
  @ApiPropertyOptional() @IsOptional() @IsString() parentId?: string | null;
  @ApiPropertyOptional() @IsOptional() @IsBoolean() isRoot?: boolean;
  @ApiProperty() @IsNumber() sortOrder: number;
}

export class BulkUpdateOrderDto {
  @ApiProperty({ type: [HierarchyOrderUpdate] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => HierarchyOrderUpdate)
  updates: HierarchyOrderUpdate[];
}

// ============================================================================
// FP&A PLATFORM DTOs (Phase 11-16)
// ============================================================================

// Variance comparison configuration
export class VarianceComparisonDto {
  @ApiProperty({ description: 'Comparison name (e.g., "Actual vs Budget")' })
  @IsString()
  @IsNotEmpty()
  name: string;

  @ApiProperty({
    description: 'Minuend system type (what we subtract from)',
    enum: ['ACTUALS', 'BUDGET', 'FORECAST', 'PRIOR_YEAR', 'CUSTOM']
  })
  @IsEnum(['ACTUALS', 'BUDGET', 'FORECAST', 'PRIOR_YEAR', 'CUSTOM'])
  minuend: 'ACTUALS' | 'BUDGET' | 'FORECAST' | 'PRIOR_YEAR' | 'CUSTOM';

  @ApiProperty({
    description: 'Subtrahend system type (what we subtract)',
    enum: ['ACTUALS', 'BUDGET', 'FORECAST', 'PRIOR_YEAR', 'CUSTOM']
  })
  @IsEnum(['ACTUALS', 'BUDGET', 'FORECAST', 'PRIOR_YEAR', 'CUSTOM'])
  subtrahend: 'ACTUALS' | 'BUDGET' | 'FORECAST' | 'PRIOR_YEAR' | 'CUSTOM';

  @ApiPropertyOptional({ description: 'Include variance percentage calculation', default: true })
  @IsOptional()
  @IsBoolean()
  includePercent?: boolean;
}

export class VarianceConfigDto {
  @ApiProperty({ description: 'Enable variance calculations' })
  @IsBoolean()
  enabled: boolean;

  @ApiProperty({ description: 'Variance comparisons', type: [VarianceComparisonDto] })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => VarianceComparisonDto)
  comparisons: VarianceComparisonDto[];
}

// Fact table configuration
export class CreateFactTableConfigDto {
  @ApiProperty({ description: 'Project ID' })
  @IsString()
  @IsNotEmpty()
  projectId: string;

  @ApiProperty({ description: 'Fact table name' })
  @IsString()
  @IsNotEmpty()
  @MinLength(1)
  factTableName: string;

  @ApiPropertyOptional({ description: 'Target database name' })
  @IsOptional()
  @IsString()
  databaseName?: string;

  @ApiPropertyOptional({ description: 'Target schema name' })
  @IsOptional()
  @IsString()
  schemaName?: string;

  @ApiPropertyOptional({ description: 'Include variance columns', default: true })
  @IsOptional()
  @IsBoolean()
  includeVariance?: boolean;

  @ApiPropertyOptional({ description: 'Variance configuration', type: VarianceConfigDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => VarianceConfigDto)
  varianceConfig?: VarianceConfigDto;

  @ApiPropertyOptional({
    description: 'System columns to include',
    enum: ['ACTUALS', 'BUDGET', 'FORECAST', 'PRIOR_YEAR', 'CUSTOM'],
    isArray: true
  })
  @IsOptional()
  @IsArray()
  @IsEnum(['ACTUALS', 'BUDGET', 'FORECAST', 'PRIOR_YEAR', 'CUSTOM'], { each: true })
  systemColumns?: ('ACTUALS' | 'BUDGET' | 'FORECAST' | 'PRIOR_YEAR' | 'CUSTOM')[];
}

export class GenerateFactTableScriptDto {
  @ApiProperty({ description: 'Project ID' })
  @IsString()
  @IsNotEmpty()
  projectId: string;

  @ApiPropertyOptional({ description: 'Hierarchy IDs to include (all if not specified)', type: [String] })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  hierarchyIds?: string[];

  @ApiProperty({ description: 'Fact table configuration', type: CreateFactTableConfigDto })
  @ValidateNested()
  @Type(() => CreateFactTableConfigDto)
  config: CreateFactTableConfigDto;

  @ApiProperty({
    description: 'Target database type',
    enum: ['snowflake', 'postgres', 'mysql', 'sqlserver']
  })
  @IsEnum(['snowflake', 'postgres', 'mysql', 'sqlserver'])
  databaseType: 'snowflake' | 'postgres' | 'mysql' | 'sqlserver';
}

// Preview query execution
export class PreviewQueryDto {
  @ApiProperty({ description: 'Connection ID' })
  @IsString()
  @IsNotEmpty()
  connectionId: string;

  @ApiProperty({ description: 'SQL query to execute' })
  @IsString()
  @IsNotEmpty()
  query: string;

  @ApiPropertyOptional({ description: 'Maximum rows to return', default: 100 })
  @IsOptional()
  @IsNumber()
  limit?: number;

  @ApiPropertyOptional({ description: 'Query parameters' })
  @IsOptional()
  @IsObject()
  params?: Record<string, any>;
}

export class EstimateRowCountDto {
  @ApiProperty({ description: 'Connection ID' })
  @IsString()
  @IsNotEmpty()
  connectionId: string;

  @ApiProperty({ description: 'SQL query to estimate' })
  @IsString()
  @IsNotEmpty()
  query: string;
}

// Excel import/export
export class ExcelImportDto {
  @ApiProperty({ description: 'Project ID' })
  @IsString()
  @IsNotEmpty()
  projectId: string;

  @ApiPropertyOptional({ description: 'Sheet name to import' })
  @IsOptional()
  @IsString()
  sheetName?: string;

  @ApiPropertyOptional({
    description: 'Column mappings from Excel to hierarchy fields',
    type: 'object'
  })
  @IsOptional()
  @IsObject()
  columnMappings?: Record<string, string>;

  @ApiPropertyOptional({
    description: 'Conflict resolution strategy',
    enum: ['merge', 'replace', 'skip'],
    default: 'merge'
  })
  @IsOptional()
  @IsEnum(['merge', 'replace', 'skip'])
  conflictResolution?: 'merge' | 'replace' | 'skip';
}

export class ExcelExportDto {
  @ApiProperty({ description: 'Project ID' })
  @IsString()
  @IsNotEmpty()
  projectId: string;

  @ApiPropertyOptional({ description: 'Hierarchy IDs to export (all if not specified)', type: [String] })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  hierarchyIds?: string[];

  @ApiPropertyOptional({ description: 'Include mappings sheet', default: true })
  @IsOptional()
  @IsBoolean()
  includeMappings?: boolean;

  @ApiPropertyOptional({ description: 'Include formulas sheet', default: true })
  @IsOptional()
  @IsBoolean()
  includeFormulas?: boolean;

  @ApiPropertyOptional({ description: 'Include variance config sheet', default: false })
  @IsOptional()
  @IsBoolean()
  includeVarianceConfig?: boolean;
}
