# Basic ideas around implementing nullable arithmetic in Julia

# Convert Nullables
import Base: convert

# Conversion
function convert{T, U}(x::Type{Nullable{T}}, y::U)
  Nullable{T}(convert(T, y))
end

# Reverse conversion
function convert{T <: Nullable, U}(x::Type{U}, y::T)
  convert(U, get(y))
end

import Base: +, -, *, /, \, ^, %, &, |, $, >>>, >>, <<, ==, !=, <, <=, >, >=

# Define binary operators
arith = [:+, :-, :*, :/, :\, :^, :%]
bit = [:&, :|, :$, :>>>, :>>, :<<]
update = [:+=, :-=, :/=, :\=, :%=, :^=, :|=, :>>>=, :>>=, :<<=]
# comp = [:(==), :!=, :<, :<=, :>, :>=]

# Symbols
syms = [arith; bit; update]

# Symbol expression union type
sym_expr = Union{Symbol, Expr}

# Nullable{T} and Nullable{T} family
function exnn{Q<:Union{Symbol, Expr}}(sym::Q)
  quote
    function $sym{T<:Nullable, U<:Nullable}(x::T, y::U)
    X = promote_type(eltype(x), eltype(y))
      try
        Nullable{X}($sym(get(x), get(y)))
      catch
        Nullable{X}()
      end
    end
  end
end

# For the Nullable{T} and U family
function exnu{Q<:Union{Symbol, Expr}}(sym::Q)
  quote
    function $sym{T<:Nullable, U}(x::T, y::U)
    X = promote_type(eltype(x), typeof(y))
       try
          Nullable{X}($sym(get(x), y))
       catch
          Nullable{X}()
       end
    end
  end
end

# For the U and Nullable{T} family
function exun{Q<:Union{Symbol, Expr}}(sym::Q)
  quote
    function $sym{U, T<:Nullable}(x::U, y::T)
    X = promote_type(typeof(x), eltype(y))
      try
        Nullable{X}($sym(x, get(y)))
      catch
        Nullable{X}()
      end
    end
  end
end


# Source the functions
for i in [exnn, exnu, exun]
  for j in syms
    eval(i(j))
  end
end
