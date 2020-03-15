//! This is a monolithic module that parses Excel values and formulas.
//! All formulas are public to allow for intermediate-parsing in the future.
//!
//! However, the return types are not very well-encapsulated. We don't want to leak the `nom` types to our callers.
//! That should be improved upon in the future.
use nom::character::complete;
use std::collections::HashMap;

use lazy_static::lazy_static;
use nom::error::ErrorKind;
use nom::lib::std::fmt::{Error, Formatter};
use nom::{branch, bytes, combinator, multi, number, sequence, AsChar, IResult};
use std::fmt::Display;

type Expr = Box<Expression>;

// TODO: Handle structured references, Sheet references.
// TODO: Perf optimizations: We should use a Memory Arena (or some other better method) instead of
// having a Boxed Self to handle recursive nature of this AST.
/// AST Representation of an Excel Formula/Value.
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Err(ExprErr), // You can actually parse these errors as valid. But we're not doing that for now.
    ValueBool(bool),
    ValueNum(f64),
    ValueString(String),
    // TODO: Box the refs to reduce memory size. Refs are currently at 40 bytes.
    RefA1(RefA1),
    Ref(Ref),
    Parens(Expr),
    Percent(Expr),
    Negate(Expr),
    Add(Expr, Expr),
    Subtract(Expr, Expr),
    Multiply(Expr, Expr),
    Divide(Expr, Expr),
    Exponentiate(Expr, Expr),
    Eq(Expr, Expr),
    NotEq(Expr, Expr),
    Gt(Expr, Expr),
    Lt(Expr, Expr),
    Gte(Expr, Expr),
    Lte(Expr, Expr),
    Concat(Expr, Expr),
    // TODO: This uses 24 bytes + 24 bytes. We should move it to a Boxed tuple.
    Fn(String, Vec<Expression>),
}

impl AsRef<Expression> for Expression {
    fn as_ref(&self) -> &Expression {
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExprErr {
    NameErr,
    RefErr,
    NumErr,
    Div0,
    NotAvailable,
}

/// Represents an A1-style reference. Commonly acquired via user input,
/// but can be transformed to a Ref later.
#[derive(Clone, Debug, PartialEq)]
pub enum RefA1 {
    CellRef(CellRefA1),
    RangeRef(Box<RefA1>, Box<RefA1>),
    UnionRef(Box<RefA1>, Box<RefA1>),
    IntersectRef(Box<RefA1>, Box<RefA1>),
    RowRangeRef((RefType, isize), (RefType, isize)),
    ColumnRangeRef((RefType, isize), (RefType, isize)),
}

/// An R0C0-based reference. Usually transformed from a RefA1.
#[derive(Clone, Debug, PartialEq)]
pub enum Ref {
    CellRef(CellRef),
    RangeRef(Box<Ref>, Box<Ref>),
    UnionRef(Box<Ref>, Box<Ref>),
    IntersectRef(Box<Ref>, Box<Ref>),
    RowRangeRef((RefType, isize), (RefType, isize)),
    ColumnRangeRef((RefType, isize), (RefType, isize)),
}

#[derive(Clone, Debug, PartialEq)]
pub enum RefType {
    Relative,
    Absolute,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CellRefA1 {
    row_reference_type: RefType,
    row: isize,
    col_reference_type: RefType,
    col: isize,
}

macro_rules! refa1_prefix {
    ( $x:expr  ) => {
        // Match zero or more comma delimited items
        {
            match $x {
                RefType::Absolute => "$",
                RefType::Relative => "",
            }
        }
    };
}

lazy_static! {
    // Use str instead of char to get around allowing empty strings.
    static ref REVERSE_ALPHABETS: HashMap<isize, &'static str> = {
        let mut set = HashMap::new();
        set.insert(1, "A");
        set.insert(2, "B");
        set.insert(3, "C");
        set.insert(4, "D");
        set.insert(5, "E");
        set.insert(6, "F");
        set.insert(7, "G");
        set.insert(8, "H");
        set.insert(9, "I");
        set.insert(10, "J");
        set.insert(11, "K");
        set.insert(12, "L");
        set.insert(13, "M");
        set.insert(14, "N");
        set.insert(15, "O");
        set.insert(16, "P");
        set.insert(17, "Q");
        set.insert(18, "R");
        set.insert(19, "S");
        set.insert(20, "T");
        set.insert(21, "U");
        set.insert(22, "V");
        set.insert(23, "W");
        set.insert(24, "X");
        set.insert(25, "Y");
        set.insert(26, "Z");
        set
    };
    static ref ALPHABETS: HashMap<char, isize> = {
        let mut set = HashMap::new();
        set.insert('A', 1);
        set.insert('B', 2);
        set.insert('C', 3);
        set.insert('D', 4);
        set.insert('E', 5);
        set.insert('F', 6);
        set.insert('G', 7);
        set.insert('H', 8);
        set.insert('I', 9);
        set.insert('J', 10);
        set.insert('K', 11);
        set.insert('L', 12);
        set.insert('M', 13);
        set.insert('N', 14);
        set.insert('O', 15);
        set.insert('P', 16);
        set.insert('Q', 17);
        set.insert('R', 18);
        set.insert('S', 19);
        set.insert('T', 20);
        set.insert('U', 21);
        set.insert('V', 22);
        set.insert('W', 23);
        set.insert('X', 24);
        set.insert('Y', 25);
        set.insert('Z', 26);
        set
    };
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Expression::Err(e) => write!(f, "{}", e),
            Expression::ValueBool(e) => write!(f, "{}", e),
            Expression::ValueString(e) => write!(f, "{}", e),
            Expression::RefA1(e) => write!(f, "{}", e),
            Expression::Ref(e) => write!(f, "{}", e),
            Expression::ValueNum(e) => write!(f, "{}", e),
            Expression::Parens(expr) => write!(f, "({})", expr),
            Expression::Percent(expr) => write!(f, "{}%", expr),
            Expression::Negate(expr) => write!(f, "-{}", expr),
            Expression::Add(expr1, expr2) => write!(f, "{} + {}", expr1, expr2),
            Expression::Subtract(expr1, expr2) => write!(f, "{} - {}", expr1, expr2),
            Expression::Multiply(expr1, expr2) => write!(f, "{} * {}", expr1, expr2),
            Expression::Divide(expr1, expr2) => write!(f, "{} / {}", expr1, expr2),
            Expression::Exponentiate(expr1, expr2) => write!(f, "{} ^ {}", expr1, expr2),
            Expression::Eq(expr1, expr2) => write!(f, "{} = {}", expr1, expr2),
            Expression::NotEq(expr1, expr2) => write!(f, "{} <> {}", expr1, expr2),
            Expression::Gt(expr1, expr2) => write!(f, "{} > {}", expr1, expr2),
            Expression::Lt(expr1, expr2) => write!(f, "{} < {}", expr1, expr2),
            Expression::Gte(expr1, expr2) => write!(f, "{} >= {}", expr1, expr2),
            Expression::Lte(expr1, expr2) => write!(f, "{} <= {}", expr1, expr2),
            Expression::Concat(expr1, expr2) => write!(f, "{} & {}", expr1, expr2),
            Expression::Fn(fn_name, exprs) => {
                write!(f, "{}(", fn_name)?;

                let exprs_len = exprs.len();

                for (index, expr) in exprs.iter().enumerate() {
                    if index != exprs_len - 1 {
                        write!(f, "{}, ", expr)?;
                    } else {
                        write!(f, "{}", expr)?;
                    }
                }

                write!(f, ")")
            }
        }
    }
}

impl Display for ExprErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            ExprErr::RefErr => write!(f, "#REF!"),
            ExprErr::NumErr => write!(f, "#NUM!"),
            ExprErr::Div0 => write!(f, "#DIV/0!"),
            ExprErr::NotAvailable => write!(f, "#N/A!"),
            ExprErr::NameErr => write!(f, "#NAME!"),
        }
    }
}

impl Display for RefA1 {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            RefA1::CellRef(cell_ref) => cell_ref.fmt(f),
            RefA1::RangeRef(ref1, ref2) => write!(f, "{}:{}", ref1, ref2),
            RefA1::UnionRef(ref1, ref2) => write!(f, "{},{}", ref1, ref2),
            RefA1::IntersectRef(ref1, ref2) => write!(f, "{} {}", ref1, ref2),
            RefA1::RowRangeRef((ref1_type, ref1), (ref2_type, ref2)) => write!(
                f,
                "{}{}:{}{}",
                refa1_prefix!(ref1_type),
                ref1,
                refa1_prefix!(ref2_type),
                ref2,
            ),
            RefA1::ColumnRangeRef((ref1_type, ref1), (ref2_type, ref2)) => write!(
                f,
                "{}{}:{}{}",
                refa1_prefix!(ref1_type),
                col_num_to_col_string(*ref1),
                refa1_prefix!(ref2_type),
                col_num_to_col_string(*ref2),
            ),
        }
    }
}

impl Display for Ref {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Ref::CellRef(cell_ref) => cell_ref.fmt(f),
            Ref::RangeRef(ref1, ref2) => write!(f, "{}:{}", ref1, ref2),
            Ref::UnionRef(ref1, ref2) => write!(f, "{},{}", ref1, ref2),
            Ref::IntersectRef(ref1, ref2) => write!(f, "{} {}", ref1, ref2),
            Ref::RowRangeRef((ref1_type, ref1), (ref2_type, ref2)) => write!(
                f,
                "R{}:R{}",
                ref1_type.display_ref(*ref1),
                ref2_type.display_ref(*ref2)
            ),
            Ref::ColumnRangeRef((ref1_type, ref1), (ref2_type, ref2)) => write!(
                f,
                "C{}:C{}",
                ref1_type.display_ref(*ref1),
                ref2_type.display_ref(*ref2)
            ),
        }
    }
}

#[allow(non_snake_case)]
impl CellRefA1 {
    pub fn new(
        row_reference_type: RefType,
        row: isize,
        col_reference_type: RefType,
        col: &str,
    ) -> Result<CellRefA1, ParseError> {
        let col = col_string_to_col(col)?;

        Ok(CellRefA1 {
            row_reference_type,
            row,
            col_reference_type,
            col,
        })
    }

    pub fn new_with_col_num(
        row_reference_type: RefType,
        row: isize,
        col_reference_type: RefType,
        col: isize,
    ) -> CellRefA1 {
        CellRefA1 {
            row_reference_type,
            row,
            col_reference_type,
            col,
        }
    }

    pub fn RRRC(row: isize, col: &str) -> Result<CellRefA1, ParseError> {
        let col = col_string_to_col(col)?;

        Ok(CellRefA1 {
            row_reference_type: RefType::Relative,
            row,
            col_reference_type: RefType::Relative,
            col,
        })
    }

    pub fn ARRC(row: isize, col: &str) -> Result<CellRefA1, ParseError> {
        let col = col_string_to_col(col)?;
        Ok(CellRefA1 {
            row_reference_type: RefType::Absolute,
            row,
            col_reference_type: RefType::Relative,
            col,
        })
    }

    pub fn RRAC(row: isize, col: &str) -> Result<CellRefA1, ParseError> {
        let col = col_string_to_col(col)?;
        Ok(CellRefA1 {
            row_reference_type: RefType::Relative,
            row,
            col_reference_type: RefType::Absolute,
            col,
        })
    }

    pub fn ARAC(row: isize, col: &str) -> Result<CellRefA1, ParseError> {
        let col = col_string_to_col(col)?;
        Ok(CellRefA1 {
            row_reference_type: RefType::Absolute,
            row,
            col_reference_type: RefType::Absolute,
            col,
        })
    }

    pub fn row(&self) -> isize {
        self.row
    }

    pub fn col(&self) -> isize {
        self.col
    }

    pub fn row_reference_type(&self) -> &RefType {
        &self.row_reference_type
    }

    pub fn col_reference_type(&self) -> &RefType {
        &self.col_reference_type
    }
}

/// This is R0C0-based.
#[derive(Clone, Debug, PartialEq)]
pub struct CellRef {
    row_reference_type: RefType,
    row: isize,
    col_reference_type: RefType,
    col: isize,
}

#[allow(non_snake_case)]
impl CellRef {
    pub fn new(
        row_reference_type: RefType,
        row: isize,
        col_reference_type: RefType,
        col: isize,
    ) -> CellRef {
        CellRef {
            row_reference_type,
            row,
            col_reference_type,
            col,
        }
    }

    pub fn RRRC(row: isize, col: isize) -> CellRef {
        CellRef {
            row_reference_type: RefType::Relative,
            row,
            col_reference_type: RefType::Relative,
            col,
        }
    }

    pub fn ARRC(row: isize, col: isize) -> CellRef {
        CellRef {
            row_reference_type: RefType::Absolute,
            row,
            col_reference_type: RefType::Relative,
            col,
        }
    }

    pub fn RRAC(row: isize, col: isize) -> CellRef {
        CellRef {
            row_reference_type: RefType::Relative,
            row,
            col_reference_type: RefType::Absolute,
            col,
        }
    }

    pub fn ARAC(row: isize, col: isize) -> CellRef {
        CellRef {
            row_reference_type: RefType::Absolute,
            row,
            col_reference_type: RefType::Absolute,
            col,
        }
    }

    pub fn row(&self) -> isize {
        self.row
    }

    pub fn row_mut(&mut self) -> &mut isize {
        &mut self.row
    }

    pub fn col(&self) -> isize {
        self.col
    }

    pub fn col_mut(&mut self) -> &mut isize {
        &mut self.col
    }

    pub fn row_reference_type(&self) -> &RefType {
        &self.row_reference_type
    }

    pub fn col_reference_type(&self) -> &RefType {
        &self.col_reference_type
    }
}

impl RefType {
    fn display_ref(&self, location: isize) -> String {
        match self {
            RefType::Absolute => format!("{}", location),
            RefType::Relative => format!("[{}]", location),
        }
    }
}

impl Display for CellRefA1 {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "{}{}{}{}",
            refa1_prefix!(self.col_reference_type),
            col_num_to_col_string(self.col),
            refa1_prefix!(self.row_reference_type),
            self.row,
        )
    }
}

impl Display for CellRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "R{}C{}",
            self.row_reference_type.display_ref(self.row),
            self.col_reference_type.display_ref(self.col)
        )
    }
}

pub fn col_string_to_col(col_string: &str) -> Result<isize, ParseError> {
    let mut result = 0;

    let mut multiplier = 0;
    for col_char in col_string.chars().rev() {
        let col_num = ALPHABETS
            .get(&col_char.as_char())
            .ok_or_else(|| ParseError(format!("Invalid column string: {}", col_string)))?;
        result = if multiplier == 0 {
            *col_num
        } else {
            result + (col_num * (ALPHABETS.len() as isize).pow(multiplier))
        };
        multiplier += 1;
    }

    Ok(result)
}

pub fn col_num_to_col_string(col_num: isize) -> String {
    let alphabets_len = REVERSE_ALPHABETS.len() as isize;

    let num_zs = col_num / alphabets_len;
    let last_char = REVERSE_ALPHABETS
        .get(&(col_num % alphabets_len))
        .unwrap_or_else(|| &"");

    format!(
        "{}{}",
        if num_zs == 0 {
            "".to_string()
        } else {
            "Z".repeat(num_zs as usize)
        },
        last_char
    )
}

#[derive(Debug, Clone)]
pub struct ParseError(String);

pub fn surround_whitespace<I, O, E: nom::error::ParseError<I>, F>(
    sep: F,
) -> impl Fn(I) -> IResult<I, O, E>
where
    F: Fn(I) -> IResult<I, O, E>,
    I: nom::InputTakeAtPosition,
    <I as nom::InputTakeAtPosition>::Item: nom::AsChar + Clone,
{
    sequence::delimited(complete::multispace0, sep, complete::multispace0)
}

pub fn parse_num_value(raw: &str) -> IResult<&str, Expression> {
    combinator::map(number::complete::double, |i: f64| Expression::ValueNum(i))(raw)
}

/// Parse out a valid string sequence within a double-quoted block. i.e. "<valid sequence>"
/// String escapes within the double-quoted block, such as "" (escaping double quotes) and "*" etc, should be handled at another layer.
/// The only reason why we escape/handle double quotes specially here is because the escape character " (double quote)
/// is the same as the requirement that a double-quoted block is deemed as a valid string syntax.
pub fn parse_string(raw: &str) -> IResult<&str, Expression> {
    let two_double_quotes = bytes::complete::tag("\"\"");
    let valid_recognize = multi::many0(branch::alt((
        two_double_quotes,
        bytes::complete::is_not("\""),
    )));

    let (rem, res) = sequence::delimited(
        bytes::complete::tag("\""),
        combinator::recognize(valid_recognize),
        bytes::complete::tag("\""),
    )(raw)?;

    Ok((rem, Expression::ValueString(String::from(res))))
}

pub fn parse_bool(raw: &str) -> IResult<&str, Expression> {
    let (rem, tagged) =
        branch::alt((bytes::complete::tag("TRUE"), bytes::complete::tag("FALSE")))(raw)?;

    match tagged {
        "TRUE" => Ok((rem, Expression::ValueBool(true))),
        "FALSE" => Ok((rem, Expression::ValueBool(false))),
        _ => panic!("Unexpected boolean tag: {}", tagged),
    }
}

pub fn is_absolute_reference(raw: &str) -> IResult<&str, &str> {
    bytes::complete::tag("$")(raw)
}

pub fn parse_column_alphabet(raw: &str) -> IResult<&str, (RefType, &str)> {
    match is_absolute_reference(raw) {
        Ok((rem, _)) => combinator::map(complete::alpha1, |s| (RefType::Absolute, s))(rem),
        Err(_) => combinator::map(complete::alpha1, |s| (RefType::Relative, s))(raw),
    }
}

pub fn to_int(raw: &str) -> IResult<&str, isize> {
    combinator::map(complete::digit1, |s: &str| s.parse::<isize>().unwrap())(raw)
}

pub fn parse_row_number(raw: &str) -> IResult<&str, (RefType, isize)> {
    match is_absolute_reference(raw) {
        Ok((rem, _)) => combinator::map(to_int, |i| (RefType::Absolute, i))(rem),
        Err(_) => combinator::map(to_int, |i| (RefType::Relative, i))(raw),
    }
}

pub fn parse_raw_cell_reference(raw: &str) -> IResult<&str, CellRefA1> {
    let (rem, (col_reference_type, matched_column)) = parse_column_alphabet(raw)?;
    let (rem, (row_reference_type, matched_row)) = parse_row_number(rem)?;
    let result = CellRefA1::new(
        row_reference_type,
        matched_row,
        col_reference_type,
        matched_column,
    );

    match result {
        Ok(res) => Ok((rem, res)),
        Err(_e) => Err(nom::Err::Error((raw, ErrorKind::Verify))),
    }
}

pub fn parse_reference_op(first: RefA1, (tag, second): (&str, RefA1)) -> RefA1 {
    match tag {
        ":" => RefA1::RangeRef(Box::new(first), Box::new(second)),
        " " => RefA1::IntersectRef(Box::new(first), Box::new(second)),
        "," => RefA1::UnionRef(Box::new(first), Box::new(second)),
        _ => panic!("Unexpected range reference tag!"),
    }
}

pub fn parse_range_reference(raw: &str) -> IResult<&str, RefA1> {
    let (rem, first_res) = parse_raw_reference(raw)?;

    let (rem, mut res_vec) = multi::many1(sequence::tuple((
        branch::alt((
            bytes::complete::tag(":"),
            bytes::complete::tag(" "),
            bytes::complete::tag(","),
        )),
        parse_raw_reference,
    )))(rem)?;

    // Ugly, but I can't seem to find a way around this, as I need the owned value from the vec.
    let rest_vec = res_vec.split_off(1);
    let mut second_res = res_vec;
    let mut res = parse_reference_op(first_res, second_res.remove(0));

    for next_res in rest_vec.into_iter() {
        res = parse_reference_op(res, next_res);
    }

    Ok((rem, res))
}

pub fn parse_raw_reference(raw: &str) -> IResult<&str, RefA1> {
    branch::alt((
        parse_row_range_reference,
        parse_col_range_reference,
        parse_cell_range_reference,
        combinator::map(parse_raw_cell_reference, |cell_ref| {
            RefA1::CellRef(cell_ref)
        }),
    ))(raw)
}

pub fn parse_reference(raw: &str) -> IResult<&str, Expression> {
    let (rem, res) = branch::alt((parse_range_reference, parse_raw_reference))(raw)?;

    Ok((rem, Expression::RefA1(res)))
}

pub fn parse_errors(raw: &str) -> IResult<&str, Expression> {
    let (rem, tagged) = branch::alt((
        bytes::complete::tag("#REF!"),
        bytes::complete::tag("#NAME!"),
        bytes::complete::tag("#NUM!"),
        bytes::complete::tag("#DIV/0!"),
        bytes::complete::tag("#N/A!"),
    ))(raw)?;

    let expr_err = match tagged {
        "#REF!" => ExprErr::RefErr,
        "#NAME!" => ExprErr::NameErr,
        "#NUM!" => ExprErr::NumErr,
        "#DIV/0!" => ExprErr::Div0,
        "#N/A!" => ExprErr::NotAvailable,
        _ => panic!("Unexpected error tag: {}", tagged),
    };

    Ok((rem, Expression::Err(expr_err)))
}

pub fn parse_cell_range_reference(raw: &str) -> IResult<&str, RefA1> {
    let (rem, (first, second)) = sequence::separated_pair(
        parse_raw_cell_reference,
        complete::char(':'),
        parse_raw_cell_reference,
    )(raw)?;

    Ok((
        rem,
        RefA1::RangeRef(
            Box::new(RefA1::CellRef(first)),
            Box::new(RefA1::CellRef(second)),
        ),
    ))
}

pub fn parse_row_range_reference(raw: &str) -> IResult<&str, RefA1> {
    let (rem, (first, second)) =
        sequence::separated_pair(parse_row_number, complete::char(':'), parse_row_number)(raw)?;

    Ok((rem, RefA1::RowRangeRef(first, second)))
}

pub fn parse_col_range_reference(raw: &str) -> IResult<&str, RefA1> {
    let (rem, ((first_ref_type, first_col_str), (second_ref_type, second_col_str))) =
        sequence::separated_pair(
            parse_column_alphabet,
            complete::char(':'),
            parse_column_alphabet,
        )(raw)?;

    let first_col = match col_string_to_col(first_col_str) {
        Ok(col) => col,
        Err(_e) => return Err(nom::Err::Error((first_col_str, ErrorKind::Verify))),
    };
    let second_col = match col_string_to_col(second_col_str) {
        Ok(col) => col,
        Err(_e) => return Err(nom::Err::Error((second_col_str, ErrorKind::Verify))),
    };

    Ok((
        rem,
        RefA1::ColumnRangeRef((first_ref_type, first_col), (second_ref_type, second_col)),
    ))
}

pub fn parse_value(raw: &str) -> IResult<&str, Expression> {
    branch::alt((
        parse_reference,
        parse_errors,
        parse_num_value,
        parse_bool,
        parse_string,
    ))(raw)
}

pub fn parse_entirely_value(raw: &str) -> IResult<&str, Expression> {
    branch::alt((combinator::all_consuming(parse_num_value), |s| {
        Ok(("", Expression::ValueString(String::from(s))))
    }))(raw)
}

pub fn parens(raw: &str) -> IResult<&str, Expression> {
    let (rem, expr) = sequence::delimited(
        complete::char('('),
        surround_whitespace(base_expr),
        complete::char(')'),
    )(raw)?;

    Ok((rem, Expression::Parens(Box::new(expr))))
}

pub fn parse_func(raw: &str) -> IResult<&str, Expression> {
    let (rem, func_name) = complete::alphanumeric1(raw)?;
    let (rem, args) = sequence::delimited(
        sequence::tuple((complete::char('('), complete::multispace0)),
        multi::separated_list(surround_whitespace(complete::char(',')), base_expr),
        sequence::tuple((complete::multispace0, complete::char(')'))),
    )(rem)?;

    Ok((rem, Expression::Fn(String::from(func_name), args)))
}

pub fn expr_level_val(raw: &str) -> IResult<&str, Expression> {
    branch::alt((parens, parse_func, parse_value))(raw)
}

pub fn expr_level_percent(raw: &str) -> IResult<&str, Expression> {
    let (rem, res) = expr_level_val(raw)?;

    if rem.starts_with("%") {
        let rem_split = rem.split_at(1).1;
        Ok((rem_split, Expression::Percent(Box::new(res))))
    } else {
        Ok((rem, res))
    }
}

pub fn expr_level_negate(raw: &str) -> IResult<&str, Expression> {
    let res: IResult<&str, &str> = bytes::complete::tag("-")(raw);

    match res {
        Ok((rem, _)) => {
            let (rem, val) = expr_level_percent(rem)?;

            Ok((rem, Expression::Negate(Box::new(val))))
        }
        Err(_) => expr_level_percent(raw),
    }
}

pub fn expr_level_exponentiate(raw: &str) -> IResult<&str, Expression> {
    let (left_rem, left_res) = expr_level_negate(raw)?;
    let (right_rem, right_res) = multi::many0(sequence::tuple((
        surround_whitespace(bytes::complete::tag("^")),
        expr_level_negate,
    )))(left_rem)?;

    Ok((right_rem, parse_expression_recur(left_res, right_res)))
}

pub fn expr_level_multiply_divide(raw: &str) -> IResult<&str, Expression> {
    let (left_rem, left_res) = expr_level_exponentiate(raw)?;
    let (right_rem, right_res) = multi::many0(sequence::tuple((
        surround_whitespace(branch::alt((
            bytes::complete::tag("*"),
            bytes::complete::tag("/"),
        ))),
        expr_level_exponentiate,
    )))(left_rem)?;

    Ok((right_rem, parse_expression_recur(left_res, right_res)))
}

pub fn expr_level_plus_minus(raw: &str) -> IResult<&str, Expression> {
    let (left_rem, left_res) = expr_level_multiply_divide(raw)?;
    let (right_rem, right_res) = multi::many0(sequence::tuple((
        surround_whitespace(branch::alt((
            bytes::complete::tag("+"),
            bytes::complete::tag("-"),
        ))),
        expr_level_multiply_divide,
    )))(left_rem)?;

    Ok((right_rem, parse_expression_recur(left_res, right_res)))
}

pub fn expr_level_concat(raw: &str) -> IResult<&str, Expression> {
    let (left_rem, left_res) = expr_level_plus_minus(raw)?;
    let (right_rem, right_res) = multi::many0(sequence::tuple((
        surround_whitespace(bytes::complete::tag("&")),
        expr_level_plus_minus,
    )))(left_rem)?;

    Ok((right_rem, parse_expression_recur(left_res, right_res)))
}

pub fn expr_level_equality(raw: &str) -> IResult<&str, Expression> {
    let (left_rem, left_res) = expr_level_concat(raw)?;
    let (right_rem, right_res) = multi::many0(sequence::tuple((
        surround_whitespace(branch::alt((
            bytes::complete::tag("="),
            bytes::complete::tag("<>"),
            bytes::complete::tag("<="),
            bytes::complete::tag(">="),
            bytes::complete::tag("<"),
            bytes::complete::tag(">"),
        ))),
        expr_level_concat,
    )))(left_rem)?;

    Ok((right_rem, parse_expression_recur(left_res, right_res)))
}

pub fn base_expr(raw: &str) -> IResult<&str, Expression> {
    expr_level_equality(raw)
}

pub fn parse_expression_recur(raw: Expression, rem: Vec<(&str, Expression)>) -> Expression {
    rem.into_iter()
        .fold(raw, |acc, val| parse_binary_op(val, acc))
}

pub fn parse_binary_op((op, expr2): (&str, Expression), expr1: Expression) -> Expression {
    match op {
        "&" => Expression::Concat(Box::new(expr1), Box::new(expr2)),
        "+" => Expression::Add(Box::new(expr1), Box::new(expr2)),
        "-" => Expression::Subtract(Box::new(expr1), Box::new(expr2)),
        "*" => Expression::Multiply(Box::new(expr1), Box::new(expr2)),
        "/" => Expression::Divide(Box::new(expr1), Box::new(expr2)),
        "^" => Expression::Exponentiate(Box::new(expr1), Box::new(expr2)),
        "=" => Expression::Eq(Box::new(expr1), Box::new(expr2)),
        "<>" => Expression::NotEq(Box::new(expr1), Box::new(expr2)),
        "<" => Expression::Lt(Box::new(expr1), Box::new(expr2)),
        "<=" => Expression::Lte(Box::new(expr1), Box::new(expr2)),
        ">" => Expression::Gt(Box::new(expr1), Box::new(expr2)),
        ">=" => Expression::Gte(Box::new(expr1), Box::new(expr2)),
        e => panic!("Unhandled operator type! {}", e),
    }
}

pub fn parse_expression(raw: &str) -> IResult<&str, Expression> {
    surround_whitespace(base_expr)(raw)
}

pub fn parse_formula(raw: &str) -> IResult<&str, Expression> {
    branch::alt((
        combinator::all_consuming(parse_num_value),
        combinator::verify(
            combinator::all_consuming(parse_expression),
            |result| match result {
                Expression::ValueNum(_) => false,
                _ => true,
            },
        ),
    ))(raw)
}

pub fn parse_raw(raw: &str) -> IResult<&str, Expression> {
    let is_formula: IResult<&str, &str> = bytes::complete::tag("=")(raw);

    match is_formula {
        Ok((formula_input, _)) => parse_formula(formula_input),
        Err(_) => parse_entirely_value(raw),
    }
}

// TODO: Better error handling.
pub fn parse_cell_content(raw: &str) -> Result<Expression, ParseError> {
    match parse_raw(raw) {
        Ok((_, expr)) => Ok(expr),
        Err(e) => match e {
            nom::Err::Incomplete(_) => Err(ParseError("Incomplete!".to_string())),
            _ => Err(ParseError(format!(
                "Something went wrong with parsing! Error:\n{:#?}",
                e
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::IResult;

    fn val_int(val: i32) -> Box<Expression> {
        Box::new(Expression::ValueNum(val as f64))
    }

    fn assert_is_err(result: IResult<&str, Expression>) {
        match result {
            Ok(res) => assert!(false, "Expected error, got: {:?}", res),
            Err(_) => assert!(true),
        }
    }

    #[test]
    fn trivial_test() {
        assert_is_err(parse_raw("=SUM"));

        assert_eq!(
            parse_raw("SUM"),
            Ok(("", Expression::ValueString(String::from("SUM"))))
        );

        assert_eq!(parse_raw("=1"), Ok(("", Expression::ValueNum(1.0))));
        assert_eq!(parse_raw("1"), Ok(("", Expression::ValueNum(1.0))));
        assert_eq!(parse_raw("=1.1"), Ok(("", Expression::ValueNum(1.1))));
        assert_eq!(parse_raw("1.1"), Ok(("", Expression::ValueNum(1.1))));
    }

    #[test]
    fn back_to_value_test() {
        assert_is_err(parse_raw("=1+1SUM"));
        assert_is_err(parse_raw("=hello(1+1,incomplete()"));
        assert_is_err(parse_raw("=1234hello"));
        assert_is_err(parse_raw("=1234hello1234"));

        assert_eq!(
            parse_raw("1+1"),
            Ok(("", Expression::ValueString(String::from("1+1"))))
        );

        assert_eq!(
            parse_raw("🐑+1"),
            Ok(("", Expression::ValueString(String::from("🐑+1"))))
        );

        assert_eq!(
            parse_raw("hello(1+1)"),
            Ok(("", Expression::ValueString(String::from("hello(1+1)"))))
        );
    }

    #[test]
    fn trivial_operator_test() {
        assert_eq!(
            parse_raw("=1+1"),
            Ok(("", Expression::Add(val_int(1), val_int(1))))
        );
        assert_eq!(
            parse_raw("=1+1+1"),
            Ok((
                "",
                Expression::Add(
                    Box::new(Expression::Add(val_int(1), val_int(1))),
                    val_int(1)
                )
            ))
        );
        assert_eq!(
            parse_raw("=1+1-1"),
            Ok((
                "",
                Expression::Subtract(
                    Box::new(Expression::Add(val_int(1), val_int(1))),
                    val_int(1)
                )
            ))
        );

        assert_eq!(
            parse_raw("=1 & 2"),
            Ok(("", Expression::Concat(val_int(1), val_int(2))))
        );

        assert_eq!(
            parse_raw(r#"="1" & "2""#),
            Ok((
                "",
                Expression::Concat(
                    Box::new(Expression::ValueString(String::from("1"))),
                    Box::new(Expression::ValueString(String::from("2"))),
                )
            ))
        );
    }

    #[test]
    fn test_equality_operators() {
        assert_eq!(
            parse_raw("=(1 = 2 <> 3 < 4) <= (4 > 5 >= 6)"),
            Ok((
                "",
                Expression::Lte(
                    Box::new(Expression::Parens(Box::new(Expression::Lt(
                        Box::new(Expression::NotEq(
                            Box::new(Expression::Eq(val_int(1), val_int(2))),
                            val_int(3)
                        )),
                        val_int(4)
                    )))),
                    Box::new(Expression::Parens(Box::new(Expression::Gte(
                        Box::new(Expression::Gt(val_int(4), val_int(5))),
                        val_int(6)
                    ))))
                )
            ))
        )
    }

    #[test]
    fn test_precedence_level_2() {
        assert_eq!(
            parse_raw("=1+1*1"),
            Ok((
                "",
                Expression::Add(
                    val_int(1),
                    Box::new(Expression::Multiply(val_int(1), val_int(1)))
                )
            ))
        );

        assert_eq!(
            parse_raw("=1*1+1"),
            Ok((
                "",
                Expression::Add(
                    Box::new(Expression::Multiply(val_int(1), val_int(1))),
                    val_int(1)
                )
            ))
        );
    }

    #[test]
    fn test_precedence_level_3() {
        assert_eq!(
            parse_raw("=1+1*1^2"),
            Ok((
                "",
                Expression::Add(
                    val_int(1),
                    Box::new(Expression::Multiply(
                        val_int(1),
                        Box::new(Expression::Exponentiate(val_int(1), val_int(2)))
                    ))
                )
            ))
        );

        assert_eq!(
            parse_raw("=2^1*1+1"),
            Ok((
                "",
                Expression::Add(
                    Box::new(Expression::Multiply(
                        Box::new(Expression::Exponentiate(val_int(2), val_int(1))),
                        val_int(1)
                    )),
                    val_int(1)
                )
            ))
        );
    }

    #[test]
    fn test_precedence_parens() {
        assert_eq!(
            parse_raw("=(1+1)*1^2"),
            Ok((
                "",
                Expression::Multiply(
                    Box::new(Expression::Parens(Box::new(Expression::Add(
                        val_int(1),
                        val_int(1)
                    )))),
                    Box::new(Expression::Exponentiate(val_int(1), val_int(2)))
                )
            ))
        );

        assert_eq!(
            parse_raw("=2^(1*1)+1"),
            Ok((
                "",
                Expression::Add(
                    Box::new(Expression::Exponentiate(
                        val_int(2),
                        Box::new(Expression::Parens(Box::new(Expression::Multiply(
                            val_int(1),
                            val_int(1)
                        )))),
                    )),
                    val_int(1)
                )
            ))
        );
    }

    #[test]
    fn test_funcall() {
        assert_eq!(
            parse_raw("=hello(1,2)"),
            Ok((
                "",
                Expression::Fn(
                    String::from("hello"),
                    vec![Expression::ValueNum(1.0), Expression::ValueNum(2.0)]
                )
            ))
        );

        assert_eq!(
            parse_raw("=hello(-1,2)"),
            Ok((
                "",
                Expression::Fn(
                    String::from("hello"),
                    vec![Expression::Negate(val_int(1)), Expression::ValueNum(2.0)]
                )
            ))
        );

        assert_eq!(
            parse_raw("=hello(1,2+1)"),
            Ok((
                "",
                Expression::Fn(
                    String::from("hello"),
                    vec![
                        Expression::ValueNum(1.0),
                        Expression::Add(val_int(2), val_int(1))
                    ]
                )
            ))
        );

        assert_eq!(
            parse_raw("=hello(1,(2+1))"),
            Ok((
                "",
                Expression::Fn(
                    String::from("hello"),
                    vec![
                        Expression::ValueNum(1.0),
                        Expression::Parens(Box::new(Expression::Add(val_int(2), val_int(1))))
                    ]
                )
            ))
        );

        assert_eq!(
            parse_raw("=hello(1,(2+1))+(1+1)"),
            Ok((
                "",
                Expression::Add(
                    Box::new(Expression::Fn(
                        String::from("hello"),
                        vec![
                            Expression::ValueNum(1.0),
                            Expression::Parens(Box::new(Expression::Add(val_int(2), val_int(1))))
                        ]
                    )),
                    Box::new(Expression::Parens(Box::new(Expression::Add(
                        val_int(1),
                        val_int(1)
                    ))))
                )
            ))
        );

        assert_eq!(
            parse_raw("=3^(hello(1,(2+1))+(1+1))"),
            Ok((
                "",
                Expression::Exponentiate(
                    val_int(3),
                    Box::new(Expression::Parens(Box::new(Expression::Add(
                        Box::new(Expression::Fn(
                            String::from("hello"),
                            vec![
                                Expression::ValueNum(1.0),
                                Expression::Parens(Box::new(Expression::Add(
                                    val_int(2),
                                    val_int(1)
                                )))
                            ]
                        )),
                        Box::new(Expression::Parens(Box::new(Expression::Add(
                            val_int(1),
                            val_int(1)
                        ))))
                    ))))
                )
            ))
        );
    }

    #[test]
    fn test_whitespaces() {
        let formula = "=   1  + 2 -  5  / (   1  *  2   ) ^ SUM( 3 , 4 )  ";
        let sum = Expression::Fn(
            String::from("SUM"),
            vec![Expression::ValueNum(3.0), Expression::ValueNum(4.0)],
        );
        let inner_multiply =
            Expression::Parens(Box::new(Expression::Multiply(val_int(1), val_int(2))));
        let exponentiation = Expression::Exponentiate(Box::new(inner_multiply), Box::new(sum));

        let divide = Expression::Divide(val_int(5), Box::new(exponentiation));
        let plus = Expression::Add(val_int(1), val_int(2));
        let minus = Expression::Subtract(Box::new(plus), Box::new(divide));

        assert_eq!(parse_raw(formula), Ok(("", minus)));
    }

    #[test]
    fn test_references() {
        assert_eq!(
            parse_raw("=A:XDW"),
            Ok((
                "",
                Expression::RefA1(RefA1::ColumnRangeRef(
                    (RefType::Relative, 1),
                    (RefType::Relative, 16351)
                ))
            ))
        );

        assert_eq!(
            parse_raw("=A1"),
            Ok((
                "",
                Expression::RefA1(RefA1::CellRef(
                    CellRefA1::new(RefType::Relative, 1, RefType::Relative, "A")
                        .unwrap_or_else(|_| panic!("Invalid col string!"))
                ))
            ))
        );
        assert_eq!(
            parse_raw("=A1:B2"),
            Ok((
                "",
                Expression::RefA1(RefA1::RangeRef(
                    Box::new(RefA1::CellRef(
                        CellRefA1::new(RefType::Relative, 1, RefType::Relative, "A")
                            .unwrap_or_else(|_| panic!("Invalid col string!"))
                    )),
                    Box::new(RefA1::CellRef(
                        CellRefA1::new(RefType::Relative, 2, RefType::Relative, "B")
                            .unwrap_or_else(|_| panic!("Invalid col string!"))
                    ))
                ))
            ))
        );

        assert_eq!(
            parse_raw("=SUM((A1:$B2), C$3:$D$4)"),
            Ok((
                "",
                Expression::Fn(
                    String::from("SUM"),
                    vec![
                        Expression::Parens(Box::new(Expression::RefA1(RefA1::RangeRef(
                            Box::new(RefA1::CellRef(
                                CellRefA1::new(RefType::Relative, 1, RefType::Relative, "A")
                                    .unwrap_or_else(|_| panic!("Invalid col string!"))
                            )),
                            Box::new(RefA1::CellRef(
                                CellRefA1::new(RefType::Relative, 2, RefType::Absolute, "B")
                                    .unwrap_or_else(|_| panic!("Invalid col string!"))
                            ))
                        )))),
                        Expression::RefA1(RefA1::RangeRef(
                            Box::new(RefA1::CellRef(
                                CellRefA1::new(RefType::Absolute, 3, RefType::Relative, "C")
                                    .unwrap_or_else(|_| panic!("Invalid col string!"))
                            )),
                            Box::new(RefA1::CellRef(
                                CellRefA1::new(RefType::Absolute, 4, RefType::Absolute, "D")
                                    .unwrap_or_else(|_| panic!("Invalid col string!"))
                            ))
                        ))
                    ]
                )
            ))
        );

        assert_eq!(
            parse_raw("=100:200"),
            Ok((
                "",
                Expression::RefA1(RefA1::RowRangeRef(
                    (RefType::Relative, 100),
                    (RefType::Relative, 200)
                ))
            ))
        );

        assert_eq!(
            parse_raw("=A:DP"),
            Ok((
                "",
                Expression::RefA1(RefA1::ColumnRangeRef(
                    (RefType::Relative, 1),
                    (RefType::Relative, 120)
                ))
            ))
        );
        assert_eq!(
            parse_raw("=SUM(A:B, $A:B, A:$B, $A:$B, 1:1, $1:1, 1:$1, $1:$1, $A2:$B$3)"),
            Ok((
                "",
                Expression::Fn(
                    String::from("SUM"),
                    vec![
                        Expression::RefA1(RefA1::ColumnRangeRef(
                            (RefType::Relative, 1),
                            (RefType::Relative, 2)
                        )),
                        Expression::RefA1(RefA1::ColumnRangeRef(
                            (RefType::Absolute, 1),
                            (RefType::Relative, 2)
                        )),
                        Expression::RefA1(RefA1::ColumnRangeRef(
                            (RefType::Relative, 1),
                            (RefType::Absolute, 2)
                        )),
                        Expression::RefA1(RefA1::ColumnRangeRef(
                            (RefType::Absolute, 1),
                            (RefType::Absolute, 2)
                        )),
                        Expression::RefA1(RefA1::RowRangeRef(
                            (RefType::Relative, 1),
                            (RefType::Relative, 1)
                        )),
                        Expression::RefA1(RefA1::RowRangeRef(
                            (RefType::Absolute, 1),
                            (RefType::Relative, 1)
                        )),
                        Expression::RefA1(RefA1::RowRangeRef(
                            (RefType::Relative, 1),
                            (RefType::Absolute, 1)
                        )),
                        Expression::RefA1(RefA1::RowRangeRef(
                            (RefType::Absolute, 1),
                            (RefType::Absolute, 1)
                        )),
                        Expression::RefA1(RefA1::RangeRef(
                            Box::new(RefA1::CellRef(
                                CellRefA1::new(RefType::Relative, 2, RefType::Absolute, "A")
                                    .unwrap_or_else(|_| panic!("Invalid col string!"))
                            )),
                            Box::new(RefA1::CellRef(
                                CellRefA1::new(RefType::Absolute, 3, RefType::Absolute, "B")
                                    .unwrap_or_else(|_| panic!("Invalid col string!"))
                            ))
                        )),
                    ]
                )
            ))
        );

        // From online resources, it looks like union operators have precedence over
        // separated args in functions.
        assert_eq!(
            parse_raw("=SUM(A1,A2)"),
            Ok((
                "",
                Expression::Fn(
                    String::from("SUM"),
                    vec![Expression::RefA1(RefA1::UnionRef(
                        Box::new(RefA1::CellRef(
                            CellRefA1::RRRC(1, "A")
                                .unwrap_or_else(|_| panic!("Invalid col string!"))
                        )),
                        Box::new(RefA1::CellRef(
                            CellRefA1::RRRC(2, "A")
                                .unwrap_or_else(|_| panic!("Invalid col string!"))
                        ))
                    ))]
                )
            ))
        );

        assert_eq!(
            parse_raw("=SUM(A1, A2)"),
            Ok((
                "",
                Expression::Fn(
                    String::from("SUM"),
                    vec![
                        Expression::RefA1(RefA1::CellRef(
                            CellRefA1::RRRC(1, "A")
                                .unwrap_or_else(|_| panic!("Invalid col string!"))
                        )),
                        Expression::RefA1(RefA1::CellRef(
                            CellRefA1::RRRC(2, "A")
                                .unwrap_or_else(|_| panic!("Invalid col string!"))
                        )),
                    ]
                )
            ))
        );

        assert_eq!(
            parse_raw("=A1:B2:1:2 C:D,Z3"),
            Ok((
                "",
                Expression::RefA1(RefA1::UnionRef(
                    Box::new(RefA1::IntersectRef(
                        Box::new(RefA1::RangeRef(
                            Box::new(RefA1::RangeRef(
                                Box::new(RefA1::CellRef(
                                    CellRefA1::RRRC(1, "A")
                                        .unwrap_or_else(|_| panic!("Invalid col string!"))
                                )),
                                Box::new(RefA1::CellRef(
                                    CellRefA1::RRRC(2, "B")
                                        .unwrap_or_else(|_| panic!("Invalid col string!"))
                                ))
                            )),
                            Box::new(RefA1::RowRangeRef(
                                (RefType::Relative, 1),
                                (RefType::Relative, 2)
                            ))
                        )),
                        Box::new(RefA1::ColumnRangeRef(
                            (RefType::Relative, 3),
                            (RefType::Relative, 4)
                        ))
                    )),
                    Box::new(RefA1::CellRef(
                        CellRefA1::RRRC(3, "Z").unwrap_or_else(|_| panic!("Invalid col string!"))
                    ))
                ))
            ))
        );

        assert_is_err(parse_raw("=A1:B2B"));
        assert_is_err(parse_raw("=A1 :B2B"));
        assert_is_err(parse_raw("=A1 : B2 B"));
    }

    #[test]
    pub fn test_string_parsing() {
        assert_eq!(
            parse_string(r#""""""#),
            Ok(("", Expression::ValueString(String::from(r#""""#))))
        );

        assert_eq!(
            parse_string(r#""The 1960's movie """"#),
            Ok((
                "",
                Expression::ValueString(String::from(r#"The 1960's movie """#))
            ))
        );

        assert_is_err(parse_raw(r#"=""""#));

        assert_eq!(
            parse_raw(r#"="hello""#),
            Ok(("", Expression::ValueString(String::from(r#"hello"#))))
        );

        assert_eq!(
            parse_raw(r#"="🐑 + 1 = hello Ș ⚓️""#),
            Ok((
                "",
                Expression::ValueString(String::from(r#"🐑 + 1 = hello Ș ⚓️"#))
            ))
        );

        // Add is not a valid syntax for string concat, just testing.
        assert_eq!(
            parse_raw(r#"="The 1960's movie """ + A1 + """ is famous""#),
            Ok((
                "",
                Expression::Add(
                    Box::new(Expression::Add(
                        // Has 2 double quotes at end, unescaped.
                        Box::new(Expression::ValueString(String::from(
                            r#"The 1960's movie """#
                        ))),
                        Box::new(Expression::RefA1(RefA1::CellRef(
                            CellRefA1::new(RefType::Relative, 1, RefType::Relative, "A")
                                .unwrap_or_else(|_| panic!("Invalid col string!"))
                        )))
                    )),
                    // Has 2 double quotes at beginning, unescaped.
                    Box::new(Expression::ValueString(String::from(r#""" is famous"#)))
                )
            ))
        );
    }

    #[test]
    fn test_bool_parsing() {
        assert_eq!(
            parse_raw("=IF(TRUE, \"TRUE\", FALSE)"),
            Ok((
                "",
                Expression::Fn(
                    "IF".to_string(),
                    vec![
                        Expression::ValueBool(true),
                        Expression::ValueString("TRUE".to_string()),
                        Expression::ValueBool(false)
                    ]
                )
            ))
        )
    }

    #[test]
    fn test_negate() {
        assert_eq!(
            parse_raw("=-SUM(1)"),
            Ok((
                "",
                Expression::Negate(Box::new(Expression::Fn(
                    String::from("SUM"),
                    vec![Expression::ValueNum(1.0)]
                )))
            ))
        )
    }

    #[test]
    fn test_percent() {
        assert_eq!(
            parse_raw("=-SUM(1)%"),
            Ok((
                "",
                Expression::Negate(Box::new(Expression::Percent(Box::new(Expression::Fn(
                    String::from("SUM"),
                    vec![Expression::ValueNum(1.0)]
                )))))
            ))
        );

        assert_eq!(
            parse_raw("=-SUM(-1% + 2%, 3%)%"),
            Ok((
                "",
                Expression::Negate(Box::new(Expression::Percent(Box::new(Expression::Fn(
                    String::from("SUM"),
                    vec![
                        Expression::Add(
                            Box::new(Expression::Negate(Box::new(Expression::Percent(Box::new(
                                Expression::ValueNum(1.0)
                            ))))),
                            Box::new(Expression::Percent(Box::new(Expression::ValueNum(2.0))))
                        ),
                        Expression::Percent(Box::new(Expression::ValueNum(3.0)))
                    ]
                )))))
            ))
        );
    }

    #[test]
    fn test_errors() {
        assert_eq!(
            parse_raw("=#REF!"),
            Ok(("", Expression::Err(ExprErr::RefErr)))
        );
        assert_eq!(
            parse_raw("=#NAME!"),
            Ok(("", Expression::Err(ExprErr::NameErr)))
        );
        assert_eq!(
            parse_raw("=#NUM!"),
            Ok(("", Expression::Err(ExprErr::NumErr)))
        );
        assert_eq!(
            parse_raw("=#DIV/0!"),
            Ok(("", Expression::Err(ExprErr::Div0)))
        );
        assert_eq!(
            parse_raw("=#N/A!"),
            Ok(("", Expression::Err(ExprErr::NotAvailable)))
        );
    }

    #[test]
    fn test_col_num_to_col_string() {
        assert_eq!(col_num_to_col_string(26), "Z".to_string());
        assert_eq!(col_num_to_col_string(27), "ZA".to_string());
        assert_eq!(col_num_to_col_string(52), "ZZ".to_string());
        assert_eq!(col_num_to_col_string(53), "ZZA".to_string());
    }
}
